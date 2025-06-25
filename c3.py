# added update and delete functionality
#Enhanced chatbot with schema-aware data insertion
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
 
# === Load environment variables ===
load_dotenv()
 
# === SAP AI Core Configuration ===
AICORE_CLIENT_ID = os.getenv("AICORE_CLIENT_ID")
AICORE_AUTH_URL = os.getenv("AICORE_AUTH_URL")
AICORE_CLIENT_SECRET = os.getenv("AICORE_CLIENT_SECRET")
AICORE_RESOURCE_GROUP = os.getenv("AICORE_RESOURCE_GROUP")
AICORE_BASE_URL = os.getenv("AICORE_BASE_URL")
LLM_DEPLOYMENT_ID = "d38dd2015862a15d"
 
def parse_tool_response(response_text):
    # First try the standard TOOL:/PARAMS: format
    tool_match = re.search(r"TOOL:\s*(\w+)", response_text)
    params_match = re.search(r"PARAMS:\s*(\{.*\})", response_text, re.DOTALL)
    if tool_match and params_match:
        tool_name = tool_match.group(1)
        params = json.loads(params_match.group(1))
        return tool_name, params
   
    # Try to parse JSON format response
    try:
        # Remove markdown code blocks if present
        cleaned_text = response_text.strip()
        if cleaned_text.startswith('```json'):
            cleaned_text = cleaned_text[7:]
        if cleaned_text.endswith('```'):
            cleaned_text = cleaned_text[:-3]
        cleaned_text = cleaned_text.strip()
       
        # Parse JSON
        json_response = json.loads(cleaned_text)
        if 'TOOL' in json_response and 'PARAMS' in json_response:
            return json_response['TOOL'], json_response['PARAMS']
    except (json.JSONDecodeError, KeyError):
        pass
   
    return None, None
 
class MCPClient:
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.tools = []
        self.memory = []
        self.cached_schema = None
 
    # === Connect to Server ===
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
 
        # List available tools
        response = await self.session.list_tools()
        self.tools = response.tools
        print("\n✅ Connected to server with tools:", [tool.name for tool in self.tools])
 
    # === Get Schema with Caching ===
    async def get_schema(self, force_refresh=False):
        """Retrieve database schema, with caching for performance"""
        if self.cached_schema and not force_refresh:
            return self.cached_schema
       
        try:
            tool_result = await self.session.call_tool("get_schema", {})
           
            # Extract schema data from tool result
            schema_text = ""
            if hasattr(tool_result, 'content') and tool_result.content:
                for content in tool_result.content:
                    if hasattr(content, 'text'):
                        schema_text = content.text
                        break
           
            if schema_text:
                try:
                    self.cached_schema = json.loads(schema_text)
                    return self.cached_schema
                except json.JSONDecodeError:
                    self.cached_schema = {"raw_schema": schema_text}
                    return self.cached_schema
        except Exception as e:
            print(f"⚠️ Warning: Could not retrieve schema - {str(e)}")
            return None
       
        return None
 
    # === Schema-Aware Data Insertion ===
    async def handle_data_insertion(self, user_input: str, schema_data: dict):
        """Handle data insertion using schema-aware LLM processing"""
       
        schema_prompt = f"""
        You are a database insertion assistant. The user wants to insert data into a database.
       
        DATABASE SCHEMA:
        {json.dumps(schema_data, indent=2)}
       
        USER INPUT: "{user_input}"
       
        TASK: Analyze the user input and create appropriate insert_data parameters based on the schema.
       
        INSTRUCTIONS:
        1. Identify which table the user wants to insert data into (default: "Customer" if unclear)
        2. Extract the data values from the user input
        3. Match the extracted data with the appropriate schema columns
        4. Handle data type conversions (strings, numbers, dates, etc.)
        5. Set reasonable defaults for missing required fields if possible
        6. Return ONLY in this exact format:
       
        TOOL: insert_data
        PARAMS: {{"table": "<table_name>", "data": {{"column1": "value1", "column2": "value2"}}}}
       
        IMPORTANT:
        - Use exact column names from the schema
        - Convert values to appropriate data types
        - For missing required fields, use reasonable defaults or ask for clarification
        - If the table name is ambiguous, use "Customer" as default
        - Ensure all JSON is properly formatted
        """
       
        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        lc_messages = [HumanMessage(content=schema_prompt)]
       
        llm_response = llm.invoke(lc_messages)
        response_text = llm_response.content
       
        # Parse the LLM response for tool call
        tool_name, params = parse_tool_response(response_text)
       
        if tool_name == "insert_data" and params:
            # Execute the insertion
            try:
                tool_result = await self.session.call_tool(tool_name, params)
               
                # Process and return user-friendly response
                return await self._process_insertion_result(user_input, tool_result, params)
               
            except Exception as e:
                return f"❌ Error inserting data: {str(e)}"
        else:
            return f"❌ Could not parse the insertion request. LLM Response: {response_text}"
 
    async def _process_insertion_result(self, original_input: str, tool_result, params: dict) -> str:
        """Process insertion result and provide user-friendly feedback"""
       
        # Extract result from tool response
        result_text = ""
        if hasattr(tool_result, 'content') and tool_result.content:
            for content in tool_result.content:
                if hasattr(content, 'text'):
                    result_text = content.text
                    break
       
        # Create a simple confirmation without using LLM
        table_name = params.get('table', 'table')
        inserted_data = params.get('data', {})
       
        # Parse the JSON response to check for success
        try:
            result_json = json.loads(result_text)
            # Check for success indicators
            if (result_json.get('message', '').lower().find('successfully') != -1 or
                result_json.get('object') == 'insert_result'):
                record_count = len(inserted_data)
                return f"✅ Successfully inserted record into {table_name} table with {record_count} fields."
            else:
                return f"❌ Error inserting data into {table_name}: {result_text}"
        except json.JSONDecodeError:
            # Fallback to text-based checking
            if ("error" in result_text.lower() or "failed" in result_text.lower()):
                return f"❌ Error inserting data into {table_name}: {result_text}"
            else:
                record_count = len(inserted_data)
                return f"✅ Successfully inserted record into {table_name} table with {record_count} fields."
 
    # === Main Query Processing ===
    async def process_query(self, query: str) -> str:
        # Build system prompt with tool descriptions
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
 
        # Check if this is a data insertion, deletion, or update request
        insertion_keywords = ['insert', 'add', 'create', 'new record', 'new row', 'save', 'store']
        deletion_keywords = ['delete', 'remove', 'drop']
        update_keywords = ['update', 'modify', 'change', 'set']
 
        lower_query = query.lower()
        if any(keyword in lower_query for keyword in insertion_keywords):
            schema_data = await self.get_schema()
            if schema_data:
                return await self.handle_data_insertion(query, schema_data)
            else:
                print("⚠️ Schema not available, using fallback method")
        elif any(keyword in lower_query for keyword in deletion_keywords):
            schema_data = await self.get_schema()
            if schema_data:
                return await self.handle_data_deletion(query, schema_data)
            else:
                print("⚠️ Schema not available, using fallback method")
        elif any(keyword in lower_query for keyword in update_keywords):
            schema_data = await self.get_schema()
            if schema_data:
                return await self.handle_data_update(query, schema_data)
            else:
                print("⚠️ Schema not available, using fallback method")
 
        # For non-insertion requests or fallback, use original processing
        system_prompt = (
            "You are a helpful assistant with access to database tools. "
            f"You have access to the following tools:\n{tool_descriptions}\n\n"
            "IMPORTANT INSTRUCTIONS:\n"
            "- When users ask about data counts, retrieving data, or querying information, use the get_data tool.\n"
            "- When users ask about table structure or schema, use the get_schema tool.\n"
            "- For data insertion requests, they will be handled by a specialized process.\n"
            "- Choose default table and schema as follows:\n"
            "  - Default Schema: SAC_1\n"
            "  - Default Table: Customer\n"
            " - Always stay with default table and schema unless specified otherwise.\n"
            "- If the user fails to mention the table name, use 'Customer' as default.\n"
            "- If you need to use a tool, respond ONLY in this exact format:\n"
            "  TOOL: <tool_name>\n"
            "  PARAMS: <JSON parameters>\n"
            "- If no tool is needed, provide a clear, natural, conversational response in plain text.\n"
            "- Never respond in JSON format unless specifically asked.\n"
            "- Be helpful, concise, and human-like in your responses.\n"
            "- Answer questions directly without unnecessary formatting.\n"
            "- When calling tools, always use the exact parameter names as defined:\n"
            "  - For delete_data, use:\n"
            "    TOOL: delete_data\n"
            "    PARAMS: {\"table\": \"<table_name>\", \"where\": {\"<column>\": \"<value>\"}}\n"
            "  - For update_data, always use:\n"
            "    TOOL: update_data\n"
            "    PARAMS: {\"table\": \"<table_name>\", \"data\": {\"<column_to_update>\": \"<new_value>\"}, \"where\": {\"<column_to_match>\": \"<match_value>\"}}\n"
        )
 
        lc_messages = [SystemMessage(content=system_prompt)]
        lc_messages.extend(self.memory)
        lc_messages.append(HumanMessage(content=query))
 
        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID, temperature=0)
        llm_response = llm.invoke(lc_messages)
        response_text = llm_response.content
 
        # Store the latest exchange in memory
        self.memory.append(HumanMessage(content=query))
        self.memory.append(llm_response)
        MAX_MEMORY = 10
        if len(self.memory) > MAX_MEMORY * 2:
            self.memory = self.memory[-MAX_MEMORY*2:]
 
        # Clean up response if it's in JSON format
        if response_text.strip().startswith('{') and response_text.strip().endswith('}'):
            try:
                json_response = json.loads(response_text)
                # Extract the actual response from common JSON structures
                for key in ['response', 'answer', 'content', 'message']:
                    if key in json_response:
                        response_text = json_response[key]
                        break
            except json.JSONDecodeError:
                pass  # If it's not valid JSON, keep original response
 
        tool_name, params = parse_tool_response(response_text)
        if tool_name:
            # Inject default table and schema if not provided
            if isinstance(params, dict):
                if 'table' not in params:
                    params['table'] = "Customer"
                if 'schema' not in params:
                    params['schema'] = "SAC_1"
 
            tool_result = await self.session.call_tool(tool_name, params)
 
            # Post-process the tool result
            processed_result = await self._process_tool_result(query, tool_name, tool_result)
            return processed_result
        else:
            return response_text
 
    async def _process_tool_result(self, original_query: str, tool_name: str, tool_result) -> str:
        """Process tool result and provide a clear, human-readable answer"""
       
        # Extract the actual data from the tool result
        result_text = ""
        if hasattr(tool_result, 'content') and tool_result.content:
            for content in tool_result.content:
                if hasattr(content, 'text'):
                    result_text = content.text
                    break
       
        if not result_text:
            return "Sorry. I couldn't retrieve the data from the tool."
       
        # Parse JSON if it's JSON data
        try:
            data = json.loads(result_text)
        except json.JSONDecodeError:
            return f"Retrieved data: {result_text}"
       
        # Use LLM to interpret the data and answer the original question
        interpretation_prompt = f"""
        The user asked: "{original_query}"
       
        The tool '{tool_name}' returned this data:
        {json.dumps(data, indent=2)}
       
        Please provide a clear, direct answer to the user's question based on this data.
        Be concise and human-friendly. Focus on answering exactly what they asked.
        If they asked for a count, give the number. If they asked for specific information, extract and present it clearly.
        Do not include JSON or technical details unless specifically requested.
        """
       
        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        lc_messages = [HumanMessage(content=interpretation_prompt)]
        interpretation_response = llm.invoke(lc_messages)
       
        return interpretation_response.content
 
    # === Chat Loop ===
    async def chat_loop(self):
        print("\n🤖 Enhanced S4HANA MCP Client Started — Type your queries or 'quit/exit' to exit.")
        print("💡 Now with schema-aware data insertion! ")
       
        while True:
            try:
                query = input("\nQuery: ").strip()
                if query.lower() in ('quit', 'exit'):
                    break
 
                response = await self.process_query(query)
                print("\n💬 Response:\n" + response)
 
            except Exception as e:
                print(f"\n❌ Error: {str(e)}")
 
    # === Cleanup ===
    async def cleanup(self):
        await self.exit_stack.aclose()
 
    # === Data Deletion ===
    async def handle_data_deletion(self, user_input: str, schema_data: dict):
        """Handle data deletion using schema-aware LLM processing"""
        schema_prompt = f"""
        You are a database assistant. The user wants to delete data from a database.
 
        DATABASE SCHEMA:
        {json.dumps(schema_data, indent=2)}
 
        USER INPUT: "{user_input}"
 
        TASK: Analyze the user input and create appropriate delete_data parameters based on the schema.
 
        INSTRUCTIONS:
        1. Identify which table the user wants to delete data from (default: "Customer" if unclear)
        2. Extract the filter conditions from the user input
        3. Match the extracted columns with the schema
        4. Return ONLY in this exact format:
 
        TOOL: delete_data
        PARAMS: {{"table": "<table_name>", "where": {{"column1": "value1"}}}}
        """
        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        lc_messages = [HumanMessage(content=schema_prompt)]
        llm_response = llm.invoke(lc_messages)
        response_text = llm_response.content
 
        tool_name, params = parse_tool_response(response_text)
        if tool_name == "delete_data" and params:
            try:
                tool_result = await self.session.call_tool(tool_name, params)
                return await self._process_deletion_result(user_input, tool_result, params)
            except Exception as e:
                return f"❌ Error deleting data: {str(e)}"
        else:
            return f"❌ Could not parse the deletion request. LLM Response: {response_text}"
 
    async def _process_deletion_result(self, original_input: str, tool_result, params: dict) -> str:
        """Process deletion result and provide user-friendly feedback"""
        result_text = ""
        if hasattr(tool_result, 'content') and tool_result.content:
            for content in tool_result.content:
                if hasattr(content, 'text'):
                    result_text = content.text
                    break
        table_name = params.get('table', 'table')
        try:
            result_json = json.loads(result_text)
            if (result_json.get('message', '').lower().find('successfully') != -1 or
                result_json.get('object') == 'delete_result'):
                return f"✅ Successfully deleted record(s) from {table_name}."
            else:
                return f"❌ Error deleting data from {table_name}: {result_text}"
        except json.JSONDecodeError:
            if ("error" in result_text.lower() or "failed" in result_text.lower()):
                return f"❌ Error deleting data from {table_name}: {result_text}"
            else:
                return f"✅ Successfully deleted record(s) from {table_name}."
 
    # === Data Update ===
    async def handle_data_update(self, user_input: str, schema_data: dict):
        """Handle data update using schema-aware LLM processing"""
        schema_prompt = f"""
        You are a database assistant. The user wants to update data in a database.
 
        DATABASE SCHEMA:
        {json.dumps(schema_data, indent=2)}
 
        USER INPUT: "{user_input}"
 
        TASK: Analyze the user input and create appropriate update_data parameters based on the schema.
 
        INSTRUCTIONS:
        1. Identify which table the user wants to update (default: "Customer" if unclear)
        2. Extract the columns to update and their new values from the user input
        3. Extract the filter conditions (where clause) from the user input
        4. Match the extracted columns with the schema
        5. Return ONLY in this exact format:
 
        TOOL: update_data
        PARAMS: {{"table": "<table_name>", "data": {{"column1": "new_value"}}, "where": {{"column2": "match_value"}}}}
        """
        llm = ChatOpenAI(deployment_id=LLM_DEPLOYMENT_ID)
        lc_messages = [HumanMessage(content=schema_prompt)]
        llm_response = llm.invoke(lc_messages)
        response_text = llm_response.content
 
        tool_name, params = parse_tool_response(response_text)
        if tool_name == "update_data" and params:
            try:
                tool_result = await self.session.call_tool(tool_name, params)
                return await self._process_update_result(user_input, tool_result, params)
            except Exception as e:
                return f"❌ Error updating data: {str(e)}"
        else:
            return f"❌ Could not parse the update request. LLM Response: {response_text}"
 
    async def _process_update_result(self, original_input: str, tool_result, params: dict) -> str:
        """Process update result and provide user-friendly feedback"""
        result_text = ""
        if hasattr(tool_result, 'content') and tool_result.content:
            for content in tool_result.content:
                if hasattr(content, 'text'):
                    result_text = content.text
                    break
        table_name = params.get('table', 'table')
        try:
            result_json = json.loads(result_text)
            if (result_json.get('message', '').lower().find('successfully') != -1 or
                result_json.get('object') == 'update_result'):
                return f"✅ Successfully updated record(s) in {table_name}."
            else:
                return f"❌ Error updating data in {table_name}: {result_text}"
        except json.JSONDecodeError:
            if ("error" in result_text.lower() or "failed" in result_text.lower()):
                return f"❌ Error updating data in {table_name}: {result_text}"
            else:
                return f"✅ Successfully updated record(s) in {table_name}."
 
# === Entry Point ===
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
 