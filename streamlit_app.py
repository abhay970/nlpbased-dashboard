import json
import streamlit as st
# from snowflake.snowpark.context import get_active_session
# from snowflake.snowpark.exceptions import SnowparkSQLException
cnx = st.connection("snowflake")
session = cnx.session()
# Config
SEMANTIC_MODEL_PATH = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_STAGE/nlp.yaml"
CHAT_PROCEDURE = "CORTEX_ANALYST.CORTEX_AI.CORTEX_ANALYST_CHAT_PROCEDURE"
DREMIO_PROCEDURE = "SALESFORCE_DREMIO.SALESFORCE_SCHEMA_DREMIO.DREMIO_DATA_PROCEDURE"

def initialize_session():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "display_messages" not in st.session_state:
        st.session_state.display_messages = []
    if "processing" not in st.session_state:
        st.session_state.processing = False

def call_cortex_analyst_procedure(messages):
    try:
        messages_json = json.dumps(messages)
        # session = get_active_session()
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
    try:
        # session = get_active_session()
        df_result = session.call(DREMIO_PROCEDURE, sql_statement)
        if hasattr(df_result, "to_pandas"):
            return df_result.to_pandas(), None
        return None, "Unexpected result format from Dremio procedure"
    except SnowparkSQLException as e:
        return None, f"Dremio SQL Error: {str(e)}"
    except Exception as e:
        return None, f"Dremio Error: {str(e)}"

def display_chat_message(role, content):
    with st.chat_message(role):
        if isinstance(content, str):
            st.markdown(content)
        elif isinstance(content, dict):
            if "message" in content:
                st.markdown(content["message"])
            if "query" in content:
                st.code(content["query"], language="sql")

def process_user_question(question):
    try:
        st.session_state.processing = True

        # Add user message to conversation history
        user_msg = {
            "role": "user",
            "content": [{"type": "text", "text": question}]
        }
        st.session_state.messages.append(user_msg)
        
        # Add to display messages for UI
        st.session_state.display_messages.append({
            "role": "user",
            "content": question
        })
        
        display_chat_message("user", question)

        with st.spinner("Analyzing your question..."):
            # Send the full conversation history including both user and analyst messages
            response, error = call_cortex_analyst_procedure(st.session_state.messages)

            if error:
                raise Exception(error)

            # Extract the analyst response
            analyst_response = response.get("message", {})
            content_block = analyst_response.get("content", [])
            
            if not isinstance(content_block, list):
                raise Exception("Invalid response structure from Cortex Analyst")

            sql_statement = None
            explanation = ""
            for block in content_block:
                if block.get("type") == "text":
                    explanation = block.get("text", "")
                elif block.get("type") == "sql" and "statement" in block:
                    sql_statement = block["statement"]

            if not sql_statement:
                raise Exception("No SQL found in response.")

            # Call Dremio
            dremio_result, dremio_error = call_dremio_data_procedure(sql_statement)
            if dremio_error:
                raise Exception(dremio_error)

            # Show response
            display_chat_message("assistant", explanation)
            display_chat_message("assistant", {"message": "Generated SQL:", "query": sql_statement})

            if dremio_result is not None and not dremio_result.empty:
                with st.chat_message("assistant"):
                    st.success("✅ Dremio executed successfully")
                    st.dataframe(dremio_result, use_container_width=True)
                    st.caption(f"{len(dremio_result)} rows × {len(dremio_result.columns)} columns")
            else:
                display_chat_message("assistant", "⚠️ No data returned from Dremio.")

            # CRITICAL: Add the analyst response to conversation history
            # This is required for multi-turn conversations
            analyst_msg = {
                "role": "analyst",
                "content": content_block  # Use the original content blocks from the API response
            }
            st.session_state.messages.append(analyst_msg)

            # Save assistant response for display
            assistant_display = f"{explanation}\n\n**Generated SQL:**\n```sql\n{sql_statement}\n```\n\n✅ Executed in Dremio."
            st.session_state.display_messages.append({
                "role": "assistant",
                "content": assistant_display
            })

    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        st.error(error_msg)
        # Add error to display messages
        st.session_state.display_messages.append({
            "role": "assistant",
            "content": error_msg
        })
    finally:
        st.session_state.processing = False

def render_chat_interface():
    st.title("🧠 Cortex Analyst")
    st.caption("Ask natural questions. Get SQL + results.")

    # Display messages from display_messages (for UI)
    for msg in st.session_state.display_messages:
        display_chat_message(msg["role"], msg["content"])

    if prompt := st.chat_input("Ask something...", disabled=st.session_state.processing):
        process_user_question(prompt)

def render_sidebar():
    pass  # Sidebar removed

def main():
    st.set_page_config(page_title="Cortex Analyst", page_icon="🧠", layout="wide")
    initialize_session()
    render_chat_interface()

if __name__ == "__main__":
    main()
