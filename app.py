import streamlit as st
import sqlite3
import pandas as pd
from openai import OpenAI
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="UberAI Navigator",
    page_icon="🚗",
    layout="wide"
)

# Custom CSS for professional look
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF5733;
        text-align: center;
        margin-bottom: 1rem;
    }
    .subheader {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stChatMessage {
        padding: 1rem;
        border-radius: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize Fireworks client
client = OpenAI(
    base_url="https://api.fireworks.ai/inference/v1",
    api_key=os.getenv("FIREWORKS_API_KEY")
)

# Database connection
def get_db_connection():
    return sqlite3.connect('uber_data.db', check_same_thread=False)

# Get schema information
@st.cache_data
def get_schema_info():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema_info = {}
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        schema_info[table_name] = [
            {"name": col[1], "type": col[2], "notnull": col[3], "pk": col[5]} 
            for col in columns
        ]
    
    conn.close()
    return schema_info

# Get PII information
@st.cache_data
def get_pii_info():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM pii_metadata", conn)
    conn.close()
    return df

# Execute SQL query
def execute_query(sql):
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

# Generate SQL using Fireworks
def generate_sql(user_question, schema_info):
    schema_text = ""
    for table, columns in schema_info.items():
        cols = ", ".join([f"{c['name']} ({c['type']})" for c in columns])
        schema_text += f"\n{table}: {cols}"
    
    prompt = f"""You are an expert SQL developer for Uber's data warehouse.

DATABASE SCHEMA:
{schema_text}

IMPORTANT RULES:
- Use SQLite syntax
- Only use tables and columns that exist in the schema above
- For date filtering, use datetime() and date() functions
- For "last week", use: date('now', '-7 days')
- Always use proper JOINs when combining tables
- Return ONLY the SQL query, no explanations

USER QUESTION: {user_question}

Generate the SQL query:"""

    start_time = time.time()
    response = client.chat.completions.create(
        model="accounts/fireworks/models/llama-v3p3-70b-instruct",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=500
    )
    latency = time.time() - start_time
    
    sql = response.choices[0].message.content.strip()
    # Clean up SQL (remove markdown code blocks if present)
    sql = sql.replace("```sql", "").replace("```", "").strip()
    
    tokens_used = response.usage.total_tokens
    cost = (response.usage.prompt_tokens * 0.20 + response.usage.completion_tokens * 0.60) / 1_000_000
    
    return sql, latency, tokens_used, cost

# Answer schema questions
def answer_schema_question(question, schema_info, pii_info):
    schema_text = ""
    for table, columns in schema_info.items():
        cols = ", ".join([f"{c['name']} ({c['type']})" for c in columns])
        schema_text += f"\n{table}: {cols}"
    
    pii_text = pii_info.to_string()
    
    prompt = f"""You are an expert on Uber's data warehouse schema and data governance.

DATABASE SCHEMA:
{schema_text}

PII METADATA:
{pii_text}

USER QUESTION: {question}

Provide a clear, concise answer about the schema, tables, columns, or PII handling.
Be specific and reference actual table/column names."""

    start_time = time.time()
    response = client.chat.completions.create(
        model="accounts/fireworks/models/llama-v3p3-70b-instruct",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=300
    )
    latency = time.time() - start_time
    
    answer = response.choices[0].message.content.strip()
    tokens_used = response.usage.total_tokens
    cost = (response.usage.prompt_tokens * 0.20 + response.usage.completion_tokens * 0.60) / 1_000_000
    
    return answer, latency, tokens_used, cost

# Classify user intent using simple keywords
def classify_intent(question):
    question_lower = question.lower()
    
    # Check for SQL query keywords
    sql_keywords = ['top', 'show', 'get', 'find', 'list', 'count', 'sum', 'average', 
                    'how many', 'total', 'select', 'where', 'last week', 'yesterday']
    
    # Check for schema/PII keywords
    schema_keywords = ['table', 'column', 'schema', 'pii', 'personal', 'privacy', 
                      'join', 'relationship', 'structure', 'database']
    
    # Count matches
    sql_matches = sum(1 for keyword in sql_keywords if keyword in question_lower)
    schema_matches = sum(1 for keyword in schema_keywords if keyword in question_lower)
    
    # Decide intent
    if sql_matches > schema_matches:
        return "sql_query"
    elif schema_matches > 0:
        return "schema_info"
    else:
        return "sql_query"  # Default to SQL query


# Main app
def main():
    # Header
    st.markdown('<div class="main-header">🚗 UberAI Navigator</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheader">Powered by Fireworks.ai 🔥</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://fireworks.ai/favicon.ico", width=50)
        st.markdown("### About")
        st.info("""
        **UberAI Navigator** is an AI-powered data assistant for Uber's analytics teams.
        
        **Capabilities:**
        - 🔍 Natural language SQL generation
        - 📊 Schema exploration
        - 🔒 PII & security guidance
        
        **Powered by Fireworks.ai:**
        - ⚡ Fast inference
        - 💰 Cost-efficient
        - 🎯 Production-ready
        """)
        
        st.markdown("---")
        st.markdown("### Quick Examples")
        st.code("What were the top 10 cities by completed rides last week?", language=None)
        st.code("What tables contain PII data?", language=None)
        st.code("How do I safely join riders and trips tables?", language=None)
    
    # Load schema and PII info
    schema_info = get_schema_info()
    pii_info = get_pii_info()
    
    # Chat interface
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "metrics" in message:
                cols = st.columns(3)
                cols[0].metric("Latency", f"{message['metrics']['latency']:.2f}s")
                cols[1].metric("Tokens", message['metrics']['tokens'])
                cols[2].metric("Cost", f"${message['metrics']['cost']:.6f}")
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about Uber's data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Classify intent
        with st.spinner("🤔 Understanding your question..."):
            intent = classify_intent(prompt)
        
        # Process based on intent
        with st.chat_message("assistant"):
            if "sql" in intent:
                # SQL query flow
                with st.spinner("🔥 Generating SQL query..."):
                    sql, latency, tokens, cost = generate_sql(prompt, schema_info)
                
                st.code(sql, language="sql")
                
                with st.spinner("⚡ Executing query..."):
                    df, error = execute_query(sql)
                
                if error:
                    st.error(f"Query Error: {error}")
                    response = f"I generated this SQL query but it failed:\n```sql\n{sql}\n```\nError: {error}"
                else:
                    st.dataframe(df, use_container_width=True)
                    st.success(f"✅ Retrieved {len(df)} rows")
                    response = f"Here's your data! I found {len(df)} results."
                
                # Show metrics
                cols = st.columns(3)
                cols[0].metric("⚡ Latency", f"{latency:.2f}s", help="Fireworks inference speed")
                cols[1].metric("🎫 Tokens", tokens, help="Total tokens used")
                cols[2].metric("💰 Cost", f"${cost:.6f}", help="Query cost")
                
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response,
                    "metrics": {"latency": latency, "tokens": tokens, "cost": cost}
                })
            
            elif "schema" in intent or "pii" in intent:
                # Schema/PII question flow
                with st.spinner("🔥 Analyzing schema..."):
                    answer, latency, tokens, cost = answer_schema_question(prompt, schema_info, pii_info)
                
                st.markdown(answer)
                
                # Show metrics
                cols = st.columns(3)
                cols[0].metric("⚡ Latency", f"{latency:.2f}s")
                cols[1].metric("🎫 Tokens", tokens)
                cols[2].metric("💰 Cost", f"${cost:.6f}")
                
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "metrics": {"latency": latency, "tokens": tokens, "cost": cost}
                })
            
            else:
                st.warning("I'm not sure how to help with that. Try asking about data queries, schema, or PII.")

if __name__ == "__main__":
    main()