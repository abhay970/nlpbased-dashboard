"""
Cortex Analyst App - SAP HANA Configuration with Salesforce Dremio Integration
============================================================================
This app allows users to interact with their data using natural language with smart data source detection.
"""
import yaml 
import json  # To handle JSON data
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Set

# import _snowflake  # For interacting with Snowflake-specific APIs
import pandas as pd
import streamlit as st  # Streamlit library for building the web app
cnx = st.connection("snowflake")
session = cnx.session()
from snowflake.snowpark.exceptions import SnowparkSQLException

# Fixed semantic model path
SEMANTIC_MODEL_PATH = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE/nlp.yaml"
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # in milliseconds

# Initialize a Snowpark session for executing queries
session = get_active_session()


@st.cache_data(show_spinner=False)
def load_semantic_model_structure():
    """
    Extract data sources, their keywords, and descriptions from the YAML semantic model.
    Returns a comprehensive structure for smart routing.
    """
    try:
        # Read the YAML file from the stage
        yaml_content = session.sql(f"SELECT GET(@{SEMANTIC_MODEL_PATH}) as content").collect()[0]['CONTENT']
        if not yaml_content:
            st.warning("YAML file is empty or not found")
            return {}
            
        semantic_model = yaml.safe_load(yaml_content)
        
        data_sources = {}
        
        # Extract from logical_models section
        for model in semantic_model.get('logical_models', []):
            source_name = model.get('name', '').strip()
            if not source_name:
                continue
                
            source_info = {
                'name': source_name,
                'description': model.get('description', '').strip(),
                'keywords': set(),
                'tables': [],
                'columns': [],
                'synonyms': set(),
                'measures': [],
                'dimensions': []
            }
            
            # Extract tables and their details
            for table in model.get('tables', []):
                table_name = table.get('name', '').strip()
                if table_name:
                    source_info['tables'].append(table_name)
                    source_info['keywords'].add(table_name.lower())
                    
                    # Add table description words as keywords
                    table_desc = table.get('description', '').strip()
                    if table_desc:
                        # Extract meaningful words from description
                        desc_words = [word.lower().strip() for word in re.findall(r'\b\w+\b', table_desc) if len(word) > 2]
                        source_info['keywords'].update(desc_words)
                
                # Extract columns
                for column in table.get('columns', []):
                    col_name = column.get('name', '').strip()
                    if col_name:
                        source_info['columns'].append(col_name)
                        source_info['keywords'].add(col_name.lower())
                        
                        # Add column description words
                        col_desc = column.get('description', '').strip()
                        if col_desc:
                            desc_words = [word.lower().strip() for word in re.findall(r'\b\w+\b', col_desc) if len(word) > 2]
                            source_info['keywords'].update(desc_words)
                    
                    # Extract synonyms
                    synonyms = column.get('synonyms', [])
                    for synonym in synonyms:
                        if synonym.strip():
                            source_info['synonyms'].add(synonym.lower().strip())
                            source_info['keywords'].add(synonym.lower().strip())
            
            # Extract measures
            for measure in model.get('measures', []):
                measure_name = measure.get('name', '').strip()
                if measure_name:
                    source_info['measures'].append(measure_name)
                    source_info['keywords'].add(measure_name.lower())
                    
                    # Add measure description words
                    measure_desc = measure.get('description', '').strip()
                    if measure_desc:
                        desc_words = [word.lower().strip() for word in re.findall(r'\b\w+\b', measure_desc) if len(word) > 2]
                        source_info['keywords'].update(desc_words)
                
                # Add synonyms from measures
                synonyms = measure.get('synonyms', [])
                for synonym in synonyms:
                    if synonym.strip():
                        source_info['synonyms'].add(synonym.lower().strip())
                        source_info['keywords'].add(synonym.lower().strip())
            
            # Extract dimensions
            for dimension in model.get('dimensions', []):
                dim_name = dimension.get('name', '').strip()
                if dim_name:
                    source_info['dimensions'].append(dim_name)
                    source_info['keywords'].add(dim_name.lower())
                    
                    # Add dimension description words
                    dim_desc = dimension.get('description', '').strip()
                    if dim_desc:
                        desc_words = [word.lower().strip() for word in re.findall(r'\b\w+\b', dim_desc) if len(word) > 2]
                        source_info['keywords'].update(desc_words)
            
            # Convert sets to lists for serialization
            source_info['keywords'] = list(source_info['keywords'])
            source_info['synonyms'] = list(source_info['synonyms'])
            
            # Only add sources that have meaningful content
            if source_info['keywords']:
                data_sources[source_name.lower()] = source_info
        
        return data_sources
        
    except Exception as e:
        st.error(f"Error loading semantic model: {e}")
        return {}


def analyze_question_for_sources(question: str, data_sources: Dict) -> Dict:
    """
    Analyze the user question to determine which data sources might be relevant.
    Returns source matches with confidence scores.
    """
    if not data_sources:
        return {}
    
    question_lower = question.lower()
    question_words = set(re.findall(r'\b\w+\b', question_lower))
    
    source_matches = {}
    
    for source_key, source_info in data_sources.items():
        source_name = source_info['name']
        keywords = set(source_info['keywords'])
        
        # Calculate match score
        matches = question_words.intersection(keywords)
        if matches:
            # Calculate confidence based on number of matches and keyword specificity
            match_score = len(matches)
            
            # Bonus for exact table/column name matches
            exact_matches = []
            for table in source_info['tables']:
                if table.lower() in question_lower:
                    exact_matches.append(f"table: {table}")
                    match_score += 2
            
            for column in source_info['columns']:
                if column.lower() in question_lower:
                    exact_matches.append(f"column: {column}")
                    match_score += 2
            
            for measure in source_info['measures']:
                if measure.lower() in question_lower:
                    exact_matches.append(f"measure: {measure}")
                    match_score += 2
            
            source_matches[source_name] = {
                'score': match_score,
                'matched_keywords': list(matches),
                'exact_matches': exact_matches,
                'description': source_info['description']
            }
    
    return source_matches


def determine_routing_strategy(source_matches: Dict, threshold: int = 2) -> Tuple[str, List[str], Dict]:
    """
    Determine if we should route directly, ask for clarification, or proceed normally.
    
    Args:
        source_matches: Dictionary of source matches with scores
        threshold: Minimum score threshold for considering a source
    
    Returns:
        Tuple of (strategy, relevant_sources, match_details)
        strategy: 'direct', 'clarify', or 'normal'
    """
    if not source_matches:
        return 'normal', [], {}
    
    # Filter sources above threshold
    relevant_sources = {name: info for name, info in source_matches.items() if info['score'] >= threshold}
    
    if len(relevant_sources) == 0:
        return 'normal', [], {}
    elif len(relevant_sources) == 1:
        # Single clear match - route directly
        source_name = list(relevant_sources.keys())[0]
        return 'direct', [source_name], relevant_sources
    else:
        # Multiple matches - ask for clarification
        sorted_sources = sorted(relevant_sources.items(), key=lambda x: x[1]['score'], reverse=True)
        source_names = [name for name, _ in sorted_sources]
        return 'clarify', source_names, relevant_sources


def create_clarification_message(question: str, relevant_sources: List[str], match_details: Dict) -> str:
    """
    Create a clarification message when multiple data sources match.
    """
    message = f"I found relevant data for your question **'{question}'** in multiple sources:\n\n"
    
    for i, source_name in enumerate(relevant_sources, 1):
        details = match_details[source_name]
        message += f"**{i}. {source_name}**"
        
        if details['description']:
            message += f" - {details['description']}"
        
        message += "\n"
        
        if details['exact_matches']:
            message += f"   • Found: {', '.join(details['exact_matches'])}\n"
        elif details['matched_keywords']:
            message += f"   • Related to: {', '.join(details['matched_keywords'][:5])}\n"  # Show max 5 keywords
        
        message += "\n"
    
    message += "**Which data source would you like me to use for your analysis?**\n\n"
    message += "Please specify the source name, or I can try to use the most relevant one automatically."
    
    return message


def create_source_enhancement_prompt(question: str, selected_source: str, data_sources: Dict) -> str:
    """
    Enhance the original question with source-specific context.
    """
    source_info = None
    for source_data in data_sources.values():
        if source_data['name'].lower() == selected_source.lower():
            source_info = source_data
            break
    
    if not source_info:
        return question
    
    enhancement = f"Focus on data from {selected_source}"
    if source_info['description']:
        enhancement += f" ({source_info['description']})"
    
    enhancement += f". Question: {question}"
    
    return enhancement


def filter_warnings(warnings: List[Dict]) -> List[Dict]:
    """
    Filter out synonym warnings and other non-critical warnings.
    
    Args:
        warnings (List[Dict]): List of warning messages
        
    Returns:
        List[Dict]: Filtered warnings
    """
    if not warnings:
        return []
    
    filtered_warnings = []
    
    for warning in warnings:
        message = warning.get('message', '').lower()
        
        # Skip synonym warnings
        if 'synonyms are duplicated' in message or 'synonym' in message:
            continue
            
        # Skip other non-critical warnings (add more patterns as needed)
        if any(skip_pattern in message for skip_pattern in [
            'may want to rename',
            'to avoid ambiguity',
            'found in columns'
        ]):
            continue
            
        # Keep important warnings
        filtered_warnings.append(warning)
    
    return filtered_warnings


def main():
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    
    # Load data sources structure
    if "data_sources" not in st.session_state:
        st.session_state.data_sources = load_semantic_model_structure()
    
    show_header_and_sidebar()
    
    if len(st.session_state.messages) == 0:
        process_user_input("What questions can I ask?")
    
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()
    display_warnings()


def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = {}  # Dictionary to store feedback submission for each request
    st.session_state.pending_clarification = None  # Store pending clarification state


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    st.title("🧠 Smart Cortex Analyst - Multi-Source Data")
    
    # Create a more dynamic description based on available sources
    data_sources = st.session_state.get('data_sources', {})
    
    if data_sources:
        st.markdown("**Available Data Sources:**")
        for source_name, source_info in data_sources.items():
            icon = "🔄"  # Default icon
            if "salesforce" in source_name.lower() or "crm" in source_name.lower():
                icon = "🔵"
            elif "odoo" in source_name.lower() or "erp" in source_name.lower():
                icon = "🟢"
            elif "sap" in source_name.lower():
                icon = "🟡"
            
            description = source_info['description'] if source_info['description'] else "Business data"
            st.markdown(f"{icon} **{source_info['name']}**: {description}")
        
        st.markdown("---")
        st.markdown(
            "💡 **How it works:** Just ask your question naturally! I'll automatically detect which data source is most relevant, "
            "or ask you to choose if your question could apply to multiple sources."
        )
    else:
        st.markdown(
            """
            Welcome to Smart Cortex Analyst! Ask questions about your data and I'll automatically recommend the best source:
            We have three different Soruce Data : Salesforce, Sap & Odoo
            The system will automatically detect the best data source for your question!
            """
        )


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt: str):
    """
    Process user input with smart data source detection and routing.

    Args:
        prompt (str): The user's input.
    """
    # Clear previous warnings at the start of a new request
    st.session_state.warnings = []
    
    # Add user message to conversation (visible to user)
    user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(user_message)
    
    # Check if this is a response to a clarification request
    if st.session_state.get('pending_clarification'):
        # User is responding to a clarification - check if they selected a source
        clarification_data = st.session_state.pending_clarification
        original_question = clarification_data['original_question']
        available_sources = clarification_data['available_sources']
        
        # Check if user mentioned any of the available sources
        selected_source = None
        for source in available_sources:
            if source.lower() in prompt.lower():
                selected_source = source
                break
        
        if selected_source:
            # User selected a source - enhance the original question
            enhanced_prompt = create_source_enhancement_prompt(
                original_question, selected_source, st.session_state.data_sources
            )
            st.session_state.pending_clarification = None
            
            # Show source selection confirmation
            with st.chat_message("assistant"):
                st.write(f"Great! I'll analyze your question using **{selected_source}** data.")
            
        else:
            # User didn't select a clear source - use the original question with best guess
            enhanced_prompt = create_source_enhancement_prompt(
                original_question, available_sources[0], st.session_state.data_sources
            )
            st.session_state.pending_clarification = None
            
            with st.chat_message("assistant"):
                st.write(f"I'll proceed with **{available_sources[0]}** as it seems most relevant.")
    else:
        # New question - perform smart routing analysis
        data_sources = st.session_state.get('data_sources', {})
        
        if data_sources and not prompt.startswith("What questions can I ask"):
            # Analyze question for data source matches
            source_matches = analyze_question_for_sources(prompt, data_sources)
            strategy, relevant_sources, match_details = determine_routing_strategy(source_matches)
            
            if strategy == 'clarify':
                # Multiple sources match - ask for clarification
                clarification_msg = create_clarification_message(prompt, relevant_sources, match_details)
                
                # Store clarification state
                st.session_state.pending_clarification = {
                    'original_question': prompt,
                    'available_sources': relevant_sources
                }
                
                # Add clarification message
                clarification_response = {
                    "role": "assistant",
                    "content": [{"type": "text", "text": clarification_msg}],
                }
                st.session_state.messages.append(clarification_response)
                st.rerun()
                return
            
            elif strategy == 'direct':
                # Single clear match - enhance with source context
                selected_source = relevant_sources[0]
                enhanced_prompt = create_source_enhancement_prompt(prompt, selected_source, data_sources)
                
                # Show automatic source selection
                with st.chat_message("assistant"):
                    st.write(f"🎯 Automatically using **{selected_source}** data for your question.")
            else:
                # No clear matches or system question - use original prompt
                enhanced_prompt = prompt
        else:
            # No data sources loaded or system question - use original prompt
            enhanced_prompt = prompt

    # Create enhanced message for API
    enhanced_message = {
        "role": "user",
        "content": [{"type": "text", "text": enhanced_prompt}],
    }
    
    # Prepare messages for API (include enhanced version)
    messages_for_api = []
    for msg in st.session_state.messages[:-1]:  # All except the last user message
        messages_for_api.append({
            "role": msg["role"],
            "content": msg["content"]
        })
    
    # Add the enhanced message
    messages_for_api.append({
        "role": enhanced_message["role"],
        "content": enhanced_message["content"]
    })

    # Show progress indicator
    with st.chat_message("analyst"):
        with st.spinner("Analyzing your question..."):
            time.sleep(1)
            response, error_msg = get_analyst_response(messages_for_api)
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True

            # Filter warnings
            if "warnings" in response:
                st.session_state.warnings = filter_warnings(response["warnings"])

            # Add the analyst message to the session state
            st.session_state.messages.append(analyst_message)
            st.rerun()


def display_warnings():
    """
    Display filtered warnings to the user.
    """
    warnings = st.session_state.warnings
    for warning in warnings:
        st.warning(warning["message"], icon="⚠️")


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Tuple[Dict, Optional[str]]: The response from the Cortex Analyst API and error message.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{SEMANTIC_MODEL_PATH}",
    }

    try:
        # Send a POST request to the Cortex Analyst API endpoint
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            API_ENDPOINT,  # path
            {},  # headers
            {},  # params
            request_body,  # body
            None,  # request_guid
            API_TIMEOUT,  # timeout in milliseconds
        )

        # Content is a string with serialized JSON object
        parsed_content = json.loads(resp["content"])

        # Check if the response is successful
        if resp["status"] < 400:
            # Return the content of the response as a JSON object
            return parsed_content, None
        else:
            # Craft readable error message
            error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content.get('request_id', 'N/A')}`
* error code: `{parsed_content.get('error_code', 'N/A')}`

Message:
```
{parsed_content.get('message', 'Unknown error')}
```

**Note:** Make sure your Cortex Analyst database (CORTEX_ANALYST), schema (CORTEX_AI), 
and stage (CORTEX_ANALYST_STAGE) exist and are accessible, and that the nlp.yaml file 
is properly uploaded to the stage.
            """
            return parsed_content, error_msg
            
    except Exception as e:
        error_msg = f"""
🚨 Connection or API error has occurred 🚨

Error: {str(e)}

**Troubleshooting tips:**
1. Ensure you're connected to the correct Snowflake account
2. Verify that database CORTEX_ANALYST exists and you have access
3. Verify that schema CORTEX_AI exists in the CORTEX_ANALYST database
4. Verify that stage CORTEX_ANALYST_STAGE exists in the CORTEX_ANALYST.CORTEX_AI
5. Ensure the nlp.yaml file is uploaded to the CORTEX_ANALYST_STAGE
6. Check your Snowflake permissions for Cortex Analyst
        """
        return {"request_id": "error"}, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message.get("request_id"))
            else:
                display_message(content, idx)


def display_message(
    content: List[Dict[str, Union[str, Dict]]],
    message_index: int,
    request_id: Union[str, None] = None,
):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
        request_id (Union[str, None]): The request ID for the message.
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Display the SQL query and send to Salesforce Dremio procedure
            display_sql_query(
                item["statement"], message_index, item.get("confidence"), request_id
            )
        else:
            # Handle other content types if necessary
            pass


def modify_salesforce_query(sql: str) -> str:
    """
    Modify SQL queries to remove 'public' schema from salesforceDb database references.
    
    Args:
        sql (str): Original SQL query
        
    Returns:
        str: Modified SQL query with public schema removed from salesforceDb references
    """
    import re
    
    # Pattern to match salesforceDb.public.table_name and replace with salesforceDb.table_name
    # This handles various cases like:
    # - salesforceDb.public.Account -> salesforceDb.Account
    # - "salesforceDb"."public"."Account" -> "salesforceDb"."Account"
    # - SALESFORCEDB.PUBLIC.ACCOUNT -> SALESFORCEDB.ACCOUNT (case insensitive)
    
    # Pattern 1: Handle quoted identifiers like "salesforceDb"."public"."table"
    pattern1 = r'("[sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB]")\.("[pP][uU][bB][lL][iI][cC]")\.'
    sql = re.sub(pattern1, r'\1.', sql)
    
    # Pattern 2: Handle unquoted identifiers like salesforceDb.public.table (case insensitive)
    pattern2 = r'\b([sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB])\.([pP][uU][bB][lL][iI][cC])\.'
    sql = re.sub(pattern2, r'\1.', sql)
    
    # Pattern 3: Handle mixed cases like "salesforceDb".public.table
    pattern3 = r'("[sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB]")\.([pP][uU][bB][lL][iI][cC])\.'
    sql = re.sub(pattern3, r'\1.', sql)
    
    # Pattern 4: Handle cases like salesforceDb."public".table
    pattern4 = r'\b([sS][aA][lL][eE][sS][fF][oO][rR][cC][eE][dD][bB])\.("[pP][uU][bB][lL][iI][cC]")\.'
    sql = re.sub(pattern4, r'\1.', sql)
    
    return sql


@st.cache_data(show_spinner=False)
def execute_dremio_procedure(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the Salesforce Dremio procedure with the generated SQL query.
    Automatically modifies salesforceDb queries to remove public schema.

    Args:
        query (str): The SQL query to pass to the procedure.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The procedure results and the error message.
    """
    global session
    try:
        # Modify the query to remove public schema from salesforceDb references
        modified_query = modify_salesforce_query(query)
        
        # Use bind variable to safely pass the query
        df = session.sql(
            "CALL SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.dremio_data_procedure(?)",
            params=[modified_query]
        ).to_pandas()
        
        return df, None
    except SnowparkSQLException as e:
        error_message = f"Salesforce Dremio procedure execution error: {str(e)}\n\n"
        
        # Check for common issues and provide helpful guidance
        error_str = str(e).lower()
        if "does not exist" in error_str:
            error_message += "Possible issues:\n"
            error_message += "• Procedure 'dremio_data_procedure' does not exist in SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO\n"
            error_message += "• Database SALESFORCE_DREMIO or schema SALESFORCE_SCHEMA_DREMIO does not exist\n"
            error_message += "• Verify the procedure name and location\n"
        elif "access denied" in error_str or "insufficient privileges" in error_str:
            error_message += "Possible issues:\n"
            error_message += "• Insufficient permissions to execute the Salesforce Dremio procedure\n"
            error_message += "• Contact your Snowflake administrator for proper grants\n"
        elif "invalid identifier" in error_str or "sql compilation error" in error_str:
            error_message += "Possible issues:\n"
            error_message += "• SQL query contains identifiers that don't exist in the target system\n"
            error_message += "• Column names in the query may not match the source schema\n"
            error_message += "• Check if table/column names need to be quoted or have different casing\n"
            error_message += f"• Query being executed: {modify_salesforce_query(query)}\n"
        else:
            error_message += "Make sure you have access to SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO and the procedure exists."
            
        return None, error_message
    except Exception as e:
        error_message = f"Unexpected error calling Salesforce Dremio procedure: {str(e)}"
        return None, error_message




def display_sql_confidence(confidence: dict):
    if confidence is None:
        return
    verified_query_used = confidence.get("verified_query_used")
    with st.popover(
        "Verified Query Used",
        help="The verified query from Verified Query Repository, used to generate the SQL",
    ):
        with st.container():
            if verified_query_used is None:
                st.text(
                    "There is no query from the Verified Query Repository used to generate this SQL answer"
                )
                return
            st.text(f"Name: {verified_query_used.get('name', 'N/A')}")
            st.text(f"Question: {verified_query_used.get('question', 'N/A')}")
            st.text(f"Verified by: {verified_query_used.get('verified_by', 'N/A')}")
            if 'verified_at' in verified_query_used:
                st.text(
                    f"Verified at: {datetime.fromtimestamp(verified_query_used['verified_at'])}"
                )
            st.text("SQL query:")
            st.write(verified_query_used.get("sql", "N/A"))


def display_sql_query(
    sql: str, message_index: int, confidence: dict, request_id: Union[str, None] = None
):
    """
    Displays the SQL query and sends it to Salesforce Dremio procedure for execution.
    Shows both original and modified queries if salesforceDb schema modification was applied.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
        confidence (dict): The confidence information of SQL query generation
        request_id (str): Request id from user request
    """
    
    # Check if query will be modified
    modified_sql = modify_salesforce_query(sql)
    query_was_modified = sql != modified_sql

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        if query_was_modified:
            st.markdown("**Original Query (from Cortex Analyst):**")
            st.code(sql, language="sql")
            st.markdown("**Modified Query (public schema removed from salesforceDb):**")
            st.code(modified_sql, language="sql")
        else:
            st.code(sql, language="sql")
        display_sql_confidence(confidence)

    # Send query to Salesforce Dremio procedure and display results
    with st.expander("Results", expanded=True):
        with st.spinner("Executing query via Salesforce Dremio procedure..."):
            df, err_msg = execute_dremio_procedure(sql)
            if df is None:
                st.error(f"Could not execute query via Salesforce Dremio procedure. Error: {err_msg}")
            elif df.empty:
                st.write("Procedure returned no data")
            else:
                # Show query results in two tabs
                data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📉"])
                with data_tab:
                    st.dataframe(df, use_container_width=True)

                with chart_tab:
                    display_charts_tab(df, message_index)
    
    if request_id and request_id != "error":
        display_feedback_section(request_id)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # There should be at least 2 columns to draw charts
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y axis",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart 📈", "Bar Chart 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("At least 2 columns are required")


def display_feedback_section(request_id: str):
    with st.popover("📝 Query Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_form_{request_id}", clear_on_submit=True):
                positive = st.radio(
                    "Rate the generated SQL", options=["👍", "👎"], horizontal=True
                )
                positive = positive == "👍"
                submit_disabled = (
                    request_id in st.session_state.form_submitted
                    and st.session_state.form_submitted[request_id]
                )

                feedback_message = st.text_input("Optional feedback message")
                submitted = st.form_submit_button("Submit", disabled=submit_disabled)
                if submitted:
                    err_msg = submit_feedback(request_id, positive, feedback_message)
                    st.session_state.form_submitted[request_id] = {"error": err_msg}
                    st.session_state.popover_open = False
                    st.rerun()
        elif (
            request_id in st.session_state.form_submitted
            and st.session_state.form_submitted[request_id]["error"] is None
        ):
            st.success("Feedback submitted", icon="✅")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])


def check_setup_status():
    """Check if the database, schema, stage, semantic model, and Salesforce Dremio procedure are properly set up."""
    global session
    
    try:
        # Check Cortex Analyst database
        db_result = session.sql("SHOW DATABASES LIKE 'CORTEX_ANALYST'").collect()
        if not db_result:
            st.error("❌ Database 'CORTEX_ANALYST' does not exist")
            return
        else:
            st.success("✅ Database 'CORTEX_ANALYST' exists")
        
        # Check Cortex AI schema
        schema_result = session.sql("SHOW SCHEMAS IN DATABASE CORTEX_ANALYST LIKE 'CORTEX_AI'").collect()
        if not schema_result:
            st.error("❌ Schema 'CORTEX_AI' does not exist in database CORTEX_ANALYST")
            return
        else:
            st.success("✅ Schema 'CORTEX_AI' exists")
        
        # Check Cortex Analyst stage
        stage_result = session.sql("SHOW STAGES IN SCHEMA CORTEX_ANALYST.CORTEX_AI LIKE 'CORTEX_ANALYST_STAGE'").collect()
        if not stage_result:
            st.error("❌ Stage 'CORTEX_ANALYST_STAGE' does not exist in CORTEX_ANALYST.CORTEX_AI")
            return
        else:
            st.success("✅ Stage 'CORTEX_ANALYST_STAGE' exists")
        
        # Check if nlp.yaml exists in stage
        try:
            files_result = session.sql("LIST @CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE").collect()
            yaml_files = [row['name'] for row in files_result if 'nlp.yaml' in row['name']]
            if not yaml_files:
                st.error("❌ File 'nlp.yaml' not found in stage CORTEX_ANALYST_STAGE")
                st.info("Upload your nlp.yaml file using: PUT file://path/to/nlp.yaml @CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE")
            else:
                st.success("✅ File 'nlp.yaml' found in stage")
        except Exception as e:
            st.warning(f"⚠️ Could not list files in stage: {str(e)}")
        
        # Check Salesforce Dremio setup
        st.divider()
        st.markdown("**Salesforce Dremio Configuration:**")
        
        # Check Salesforce Dremio database
        try:
            sf_db_result = session.sql("SHOW DATABASES LIKE 'SALESFORCE_DREMIO'").collect()
            if not sf_db_result:
                st.error("❌ Database 'SALESFORCE_DREMIO' does not exist")
            else:
                st.success("✅ Database 'SALESFORCE_DREMIO' exists")
        except Exception as e:
            st.error(f"❌ Error checking SALESFORCE_DREMIO database: {str(e)}")
        
        # Check Salesforce Dremio schema
        try:
            sf_schema_result = session.sql("SHOW SCHEMAS IN DATABASE SALESFORCE_DREMIO LIKE 'SALESFORCE_SCHEMA_DREMIO'").collect()
            if not sf_schema_result:
                st.error("❌ Schema 'SALESFORCE_SCHEMA_DREMIO' does not exist in database SALESFORCE_DREMIO")
            else:
                st.success("✅ Schema 'SALESFORCE_SCHEMA_DREMIO' exists")
        except Exception as e:
            st.error(f"❌ Error checking SALESFORCE_SCHEMA_DREMIO schema: {str(e)}")
        
        # Check Salesforce Dremio procedure
        try:
            proc_result = session.sql("SHOW PROCEDURES IN SCHEMA SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO LIKE 'dremio_data_procedure'").collect()
            if not proc_result:
                st.error("❌ Procedure 'dremio_data_procedure' does not exist in SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO")
            else:
                st.success("✅ Procedure 'dremio_data_procedure' exists")
        except Exception as e:
            st.error(f"❌ Error checking dremio_data_procedure: {str(e)}")
        
        # General info
        st.info("💡 If setup looks good but queries fail, check that:\n"
                "• Your nlp.yaml semantic model references the correct table names\n"
                "• The Salesforce Dremio procedure is properly configured\n"
                "• You have EXECUTE permissions on the procedure\n"
                "• SalesforceDb queries will automatically have 'public' schema removed")
                
    except Exception as e:
        st.error(f"❌ Error checking setup: {str(e)}")


def submit_feedback(
    request_id: str, positive: bool, feedback_message: str
) -> Optional[str]:
    """
    Submit feedback for a query.
    
    Args:
        request_id (str): The request ID.
        positive (bool): Whether the feedback is positive.
        feedback_message (str): Optional feedback message.
        
    Returns:
        Optional[str]: Error message if submission failed, None if successful.
    """
    request_body = {
        "request_id": request_id,
        "positive": positive,
        "feedback_message": feedback_message,
    }
    
    try:
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            FEEDBACK_API_ENDPOINT,  # path
            {},  # headers
            {},  # params
            request_body,  # body
            None,  # request_guid
            API_TIMEOUT,  # timeout in milliseconds
        )
        if resp["status"] == 200:
            return None

        parsed_content = json.loads(resp["content"])
        # Craft readable error message
        err_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content.get('request_id', 'N/A')}`
* error code: `{parsed_content.get('error_code', 'N/A')}`

Message:
```
{parsed_content.get('message', 'Unknown error')}
```
        """
        return err_msg
        
    except Exception as e:
        return f"Error submitting feedback: {str(e)}"


if __name__ == "__main__":
    main()
