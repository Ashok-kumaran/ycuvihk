import os
import sys
import asyncio
import json
import re
from typing import Optional
from contextlib import AsyncExitStack
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI
from langchain.schema.messages import HumanMessage, SystemMessage
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from dotenv import load_dotenv

load_dotenv()

AICORE_CLIENT_ID = os.getenv("AICORE_CLIENT_ID")
AICORE_AUTH_URL = os.getenv("AICORE_AUTH_URL")
AICORE_CLIENT_SECRET = os.getenv("AICORE_CLIENT_SECRET")
AICORE_RESOURCE_GROUP = os.getenv("AICORE_RESOURCE_GROUP")
AICORE_BASE_URL = os.getenv("AICORE_BASE_URL")
LLM_DEPLOYMENT_ID = "d38dd2015862a15d"

def parse_tool_response(response_text):
    tool_match = re.search(r"TOOL:\s*(\w+)", response_text)
    params_match = re.search(r"PARAMS:\s*(\{.*\})", response_text, re.DOTALL)
    if tool_match and params_match:
        tool_name = tool_match.group(1)
        params = json.loads(params_match.group(1))
        return tool_name, params
    return None, None

# Helper to extract key-value pairs from messy text
def extract_fields(text):
    pattern = r'([A-Za-z0-9 \-\(\)/]+):\s*([^\n]+?)(?=(?:[A-Za-z0-9 \-\(\)/]+:)|$)'
    matches = re.findall(pattern, text)
    return {k.strip(): v.strip() for k, v in matches}

class MCPClient:
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.tools = []
        self.memory = []
        self.schema_text = ""

    async def connect_to_server(self, server_script_path: str):
        is_python = server_script_path.endswith('.py')
        is_js = server_script_path.endswith('.js')
        if not (is_python or is_js):
            raise ValueError("Server script must be a .py or .js file")

        command = "python" if is_python else "node"
        server_params = StdioServerParameters(
            command=command,
            args=[server_script_path],
            env=None
        )

        stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        self.stdio, self.write = stdio_transport
        self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))

        await self.session.initialize()

        response = await self.session.list_tools()
        self.tools = response.tools
        print("\n✅ Connected to server with tools:", [tool.name for tool in self.tools])

        # Fetch schema for LLM prompt enrichment
        try:
            schema_result = await self.session.call_tool("get_schema", {})
            if hasattr(schema_result, 'content') and schema_result.content:
                for content in schema_result.content:
                    if hasattr(content, 'text'):
                        self.schema_text = content.text
                        break
        except Exception as e:
            print(f"⚠️ Failed to fetch schema: {e}")

    async def process_query(self, query: str) -> str:
        extracted_data = extract_fields(query)
        if extracted_data:
            formatted_data = "\n".join([f"{k}: {v}" for k, v in extracted_data.items()])
            query = f"Add this data to the Customer table:\n{formatted_data}"

        def format_tool_params(tool):
            if hasattr(tool, 'input_schema') and tool.input_schema and 'properties' in tool.input_schema:
                params = [f'{name}: {prop.get("type", "any")}' for name, prop in tool.input_schema['properties'].items()]
                return ', '.join(params)
            elif hasattr(tool, 'parameters') and tool.parameters:
                if isinstance(tool.parameters, list):
                    params = [f'{param.name}: {param.type}' for param in tool.parameters if hasattr(param, 'name') and hasattr(param, 'type')]
                    return ', '.join(params)
            return ''

        tool_descriptions = "\n".join([
            f"- {tool.name}({format_tool_params(tool)}): {tool.description}"
            for tool in self.tools
        ])

        system_prompt = (
            "You are a helpful assistant with access to database tools. Your primary purpose is to add new rows into the table. "
            "You have access to the following tools:\n"
            f"{tool_descriptions}\n\n"
            f"Here is the current table schema for your reference:\n{self.schema_text}\n\n"
            "IMPORTANT INSTRUCTIONS:\n"
            "- When users ask to INSERT, ADD, CREATE data or row or record to a table/database, you MUST use the insert_data tool.\n"
            "- When users ask about data counts, retrieving data, or querying information, use the get_data tool.\n"
            "- When users ask about table structure or schema, use the get_schema tool.\n"
            "- Default Schema: SAC_1\n"
            "- Default Table: Customer\n"
            "- When calling tools, ALWAYS use this exact format:\n"
            "  TOOL: <tool_name>\n"
            "  PARAMS: <JSON parameters>\n"
            "- Never respond with JSON unless following the PARAMS format exactly.\n"
            
            "- Be concise, helpful, and avoid SQL examples.\n"
        )

        lc_messages = [SystemMessage(content=system_prompt)]
        lc_messages.extend(self.memory)
        lc_messages.append(HumanMessage(content=query))

        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        llm_response = llm.invoke(lc_messages)
        response_text = llm_response.content

        self.memory.append(HumanMessage(content=query))
        self.memory.append(llm_response)
        MAX_MEMORY = 10
        if len(self.memory) > MAX_MEMORY * 2:
            self.memory = self.memory[-MAX_MEMORY*2:]

        if response_text.strip().startswith('{') and response_text.strip().endswith('}'):
            try:
                json_response = json.loads(response_text)
                for key in ['response', 'answer', 'content', 'message']:
                    if key in json_response:
                        response_text = json_response[key]
                        break
            except json.JSONDecodeError:
                pass

        tool_name, params = parse_tool_response(response_text)

        # Handle plain tool call like "get_schema()"
        if not tool_name:
            tool_call_match = re.match(r"get_schema\s*\(\s*\)?", response_text.strip(), re.IGNORECASE)
            if tool_call_match:
                tool_name = "get_schema"
                params = {"table": "Customer", "schema": "SAC_1"}

        if tool_name:
            if isinstance(params, dict):
                params.setdefault('table', 'Customer')
                params.setdefault('schema', 'SAC_1')

            tool_result = await self.session.call_tool(tool_name, params)
            processed_result = await self._process_tool_result(query, tool_name, tool_result)
            return processed_result
        else:
            return response_text

    async def _process_tool_result(self, original_query: str, tool_name: str, tool_result) -> str:
        result_text = ""
        if hasattr(tool_result, 'content') and tool_result.content:
            for content in tool_result.content:
                if hasattr(content, 'text'):
                    result_text = content.text
                    break

        if not result_text:
            return "Sorry. I couldn't retrieve the data from the tool."

        try:
            data = json.loads(result_text)
        except json.JSONDecodeError:
            return f"Retrieved data: {result_text}"

        interpretation_prompt = (
            f"The user asked: \"{original_query}\"\n\n"
            f"The tool '{tool_name}' returned this data:\n{json.dumps(data, indent=2)}\n\n"
            "Please provide a clear, direct answer to the user's question based on this data."
        )

        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        lc_messages = [HumanMessage(content=interpretation_prompt)]
        interpretation_response = llm.invoke(lc_messages)

        return interpretation_response.content

    async def chat_loop(self):
        print("\n🤖 S4HANA MCP Client Started — Type your queries or 'quit/exit' to exit.")
        while True:
            try:
                query = input("\nQuery: ").strip()
                if query.lower() in ('quit', 'exit'):
                    break

                response = await self.process_query(query)
                print("\n💬 Response:\n" + response)

            except Exception as e:
                print(f"\n❌ Error: {str(e)}")

    async def cleanup(self):
        await self.exit_stack.aclose()

async def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_server_script>")
        sys.exit(1)

    client = MCPClient()
    try:
        await client.connect_to_server(sys.argv[1])
        await client.chat_loop()
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
