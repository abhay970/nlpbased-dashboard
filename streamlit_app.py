"""
Cortex Analyst Chat Interface
============================
Streamlit app for interacting with Cortex Analyst through Snowflake stored procedures.
"""

import json
import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
cnx = st.connection("snowflake")
session = cnx.session()
# Configuration
SEMANTIC_MODEL_PATH = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE/nlp.yaml"
CHAT_PROCEDURE = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_CHAT_PROCEDURE"

def initialize_session():
    """Initialize session state variables."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "processing" not in st.session_state:
        st.session_state.processing = False

def call_cortex_analyst_procedure(messages):
    """
    Call the Cortex Analyst stored procedure.
    
    Args:
        messages: List of conversation messages
        
    Returns:
        Tuple of (response_dict, error_message)
    """
    try:
        # Convert messages to JSON string
        messages_json = json.dumps(messages)
        
        # Call the stored procedure
        session = get_active_session()
        result = session.call(
            CHAT_PROCEDURE,
            messages_json,
            SEMANTIC_MODEL_PATH
        )
        
        if not result:
            return None, "No response from procedure"
        
        # Parse the procedure response
        procedure_response = json.loads(result)
        
        if procedure_response.get("success", False):
            return procedure_response.get("content", {}), None
        else:
            error_msg = procedure_response.get("error_message", "Unknown procedure error")
            return None, error_msg
            
    except SnowparkSQLException as e:
        error_msg = f"Database Error: {str(e)}"
        return None, error_msg
        
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON response from procedure: {str(e)}"
        return None, error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        return None, error_msg

def display_chat_message(role, content):
    """Display a chat message in the UI."""
    with st.chat_message(role):
        if isinstance(content, str):
            st.markdown(content)
        elif isinstance(content, dict):
            if "message" in content:
                st.markdown(content["message"])
            if "query" in content:
                st.code(content["query"], language="sql")

def process_user_question(question):
    """Process user question through Cortex Analyst."""
    try:
        st.session_state.processing = True
        
        # Add user message to history
        user_message = {
            "role": "user",
            "content": [{"type": "text", "text": question}]
        }
        st.session_state.messages.append(user_message)
        
        # Display user message immediately
        display_chat_message("user", question)
        
        # Get response from procedure
        with st.spinner("Analyzing your question..."):
            response, error = call_cortex_analyst_procedure(st.session_state.messages)
            
            if error:
                raise Exception(error)
            
            # Display assistant response
            display_chat_message("assistant", response)
            
            # Add assistant message to history
            assistant_message = {
                "role": "assistant",
                "content": [{"type": "text", "text": response.get("message", "")}]
            }
            if "query" in response:
                assistant_message["content"].append({
                    "type": "code",
                    "text": response["query"],
                    "language": "sql"
                })
            st.session_state.messages.append(assistant_message)
            
    except Exception as e:
        st.error(f"Error processing your question: {str(e)}")
    finally:
        st.session_state.processing = False

def render_chat_interface():
    """Render the main chat interface."""
    st.title("🧠 Cortex Analyst")
    st.caption("Ask questions about your data and get SQL-powered answers")
    
    # Display conversation history
    for message in st.session_state.messages:
        content = ""
        for content_part in message["content"]:
            if content_part["type"] == "text":
                content += content_part["text"] + "\n\n"
            elif content_part["type"] == "code":
                content += f"```{content_part.get('language', '')}\n{content_part['text']}\n```"
        
        display_chat_message(message["role"], content.strip())
    
    # Chat input
    if prompt := st.chat_input(
        "Ask a question about your data...",
        disabled=st.session_state.processing
    ):
        process_user_question(prompt)

def render_sidebar():
    """Render the sidebar controls."""
    with st.sidebar:
        st.header("Controls")
        
        if st.button("🔄 Clear Conversation", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
        
        st.divider()
        st.markdown("**About**")
        st.markdown("""
        This interface connects to Snowflake's Cortex Analyst
        through stored procedures to provide natural language
        to SQL capabilities.
        """)

def main():
    """Main app function."""
    st.set_page_config(
        page_title="Cortex Analyst",
        page_icon="🧠",
        layout="wide"
    )
    
    initialize_session()
    render_sidebar()
    render_chat_interface()

if __name__ == "__main__":
    main()
