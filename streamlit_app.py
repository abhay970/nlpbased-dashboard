import json
import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
# Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()
# Configuration
SEMANTIC_MODEL_PATH = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE/nlp.yaml"
CHAT_PROCEDURE = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_CHAT_PROCEDURE"
DREMIO_PROCEDURE = "SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.DREMIO_DATA_PROCEDURE"

def initialize_session():
    """Initialize session state variables."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "processing" not in st.session_state:
        st.session_state.processing = False

def call_cortex_analyst_procedure(messages):
    """Call the Cortex Analyst stored procedure."""
    try:
        messages_json = json.dumps(messages)
        
        result = session.call(CHAT_PROCEDURE, messages_json, SEMANTIC_MODEL_PATH)

        if not result:
            return None, "No response from procedure"
        procedure_response = json.loads(result)
        if procedure_response.get("success", False):
            return procedure_response.get("content", {}), None
        else:
            return None, procedure_response.get("error_message", "Unknown procedure error")
    except SnowparkSQLException as e:
        return None, f"Database Error: {str(e)}"
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON response: {str(e)}"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

def call_dremio_data_procedure(sql_statement):
    """Call the DREMIO_DATA_PROCEDURE and return Pandas DataFrame."""
    try:
        session = get_active_session()
        df_result = session.call(DREMIO_PROCEDURE, sql_statement)

        # Convert Snowpark DataFrame to Pandas DataFrame
        if hasattr(df_result, "to_pandas"):
            return df_result.to_pandas(), None
        else:
            return None, "Unexpected result format from Dremio procedure"
    except SnowparkSQLException as e:
        return None, f"Dremio Procedure SQL Error: {str(e)}"
    except Exception as e:
        return None, f"Unexpected Dremio Error: {str(e)}"

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
    """Process user question through Cortex Analyst and Dremio."""
    try:
        st.session_state.processing = True

        # Add user message
        user_message = {
            "role": "user",
            "content": [{"type": "text", "text": question}]
        }
        st.session_state.messages.append(user_message)
        display_chat_message("user", question)

        with st.spinner("Analyzing your question..."):
            response, error = call_cortex_analyst_procedure(st.session_state.messages)
            if error:
                raise Exception(error)

            # Extract SQL statement and explanation
            sql_statement = None
            explanation = ""
            message_blocks = response.get("message", {}).get("content", [])
            for block in message_blocks:
                if block.get("type") == "text":
                    explanation = block.get("text", "")
                if block.get("type") == "sql" and "statement" in block:
                    sql_statement = block["statement"]

            if not sql_statement:
                raise Exception("No SQL statement found in response.")

            # Call Dremio procedure
            dremio_result, dremio_error = call_dremio_data_procedure(sql_statement)
            if dremio_error:
                raise Exception(dremio_error)

            # Display explanation and SQL
            display_chat_message("assistant", explanation)
            display_chat_message("assistant", {"message": "Generated SQL:", "query": sql_statement})

            # Display Dremio result as table
            if dremio_result is not None and not dremio_result.empty:
                with st.chat_message("assistant"):
                    st.success("✅ Dremio procedure executed successfully. Results below:")
                    st.dataframe(dremio_result, use_container_width=True)
                    st.caption(f"Returned {len(dremio_result)} rows × {len(dremio_result.columns)} columns.")
            else:
                display_chat_message("assistant", "⚠️ No data returned from Dremio procedure.")

            # Save assistant messages
            st.session_state.messages.append({
                "role": "assistant",
                "content": [
                    {"type": "text", "text": explanation},
                    {"type": "code", "text": sql_statement, "language": "sql"},
                    {"type": "text", "text": "✅ Executed and displayed result from Dremio."}
                ]
            })

    except Exception as e:
        st.error(f"Error: {str(e)}")
    finally:
        st.session_state.processing = False

def render_chat_interface():
    """Render the main chat interface."""
    st.title("🧠 Cortex Analyst")
    st.caption("Ask questions about your data and get SQL-powered answers")

    # Replay conversation
    for message in st.session_state.messages:
        content = ""
        for part in message["content"]:
            if part["type"] == "text":
                content += part["text"] + "\n\n"
            elif part["type"] == "code":
                content += f"```{part.get('language', '')}\n{part['text']}\n```"
        display_chat_message(message["role"], content.strip())

    # Input
    if prompt := st.chat_input("Ask a question about your data...", disabled=st.session_state.processing):
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
        This app connects to Snowflake Cortex Analyst to generate SQL and executes it via 
        SALESFORCE_DREMIO.DREMIO_DATA_PROCEDURE to render results in table format.
        """)

def main():
    st.set_page_config(page_title="Cortex Analyst", page_icon="🧠", layout="wide")
    initialize_session()
    render_sidebar()
    render_chat_interface()

if __name__ == "__main__":
    main()
