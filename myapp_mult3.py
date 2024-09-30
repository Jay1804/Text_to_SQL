import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from langchain import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain_google_genai import GoogleGenerativeAI
from dotenv import load_dotenv
from sqlalchemy.exc import ProgrammingError
import re  # For extracting table names

# Load environment variables from env.txt file
load_dotenv(dotenv_path='env.txt')

# Database connection parameters
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "Jay%401804")
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "imdb")

# Create SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

# Initialize SQLDatabase
db = SQLDatabase(engine, sample_rows_in_table_info=3)

# Fetch Google API key from environment variables
google_api_key = os.getenv("GOOGLE_API_KEY")

if not google_api_key:
    st.error("GOOGLE_API_KEY not found. Please set the key in your environment.")
else:
    # Initialize LLM (Google Gemini Pro via Generative AI)
    llm = GoogleGenerativeAI(model="gemini-pro", google_api_key=google_api_key)

    # Create SQL query chain
    chain = create_sql_query_chain(llm, db)

    def extract_tables_from_query(sql_query):
        """
        Extract table names from the SQL query using regex.
        This version handles multiple patterns and spaces.
        """
        # Debugging: Log the SQL query
        st.write("Debugging: SQL Query to Extract Tables:")
        st.code(sql_query)

        # Regex to find table names in the SQL query
        table_pattern = re.compile(r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?=\s|$|;)", re.IGNORECASE)

        # Extract table names from the query
        tables_found = set(re.findall(table_pattern, sql_query))

        # Debugging: print extracted tables
        st.write("Extracted Tables (FROM and JOIN):", tables_found)

        return tables_found

    def execute_query(question):
        try:
            # Generate SQL query from the question
            response = chain.invoke({"question": question})
            st.write("Raw LLM Response:")
            st.text(response)  # Debugging line to see the raw response

            # Strip the formatting markers from the response
            cleaned_query = response.strip('```sql\n').strip('\n```')
            st.write("Cleaned SQL Query:")
            st.code(cleaned_query, language="sql")  # Display the cleaned SQL query

            # Extract table names from the query
            used_tables = extract_tables_from_query(cleaned_query)

            if not used_tables:
                st.write("No tables found in the query. Please check the query structure.")

            # Execute the cleaned query
            result = db.run(cleaned_query)
            return cleaned_query, result, used_tables  # Return cleaned query, result, and used tables
        except ProgrammingError as e:
            st.error(f"An error occurred: {e}")
            return None, None, None

    # Streamlit interface
    st.title("Question Answering with SQL Queries")

    # Input from user
    st.subheader("Ask a Question about the Data")
    question = st.text_input("Enter your question:")

    if st.button("Execute"):
        if question:
            cleaned_query, query_result, used_tables = execute_query(question)
            
            if cleaned_query and query_result is not None:
                st.write("Generated SQL Query:")
                st.code(cleaned_query, language="sql")  # Display SQL query in code format
                st.write("Query Result:")
                
                # Check if the result is a DataFrame
                if isinstance(query_result, pd.DataFrame):
                    st.dataframe(query_result, use_container_width=True)  # Display results in a grid-like format
                else:
                    st.write(query_result)  # Fallback for non-DataFrame results

                # Show schema only for the tables used in the query
                if used_tables:
                    with st.expander("View Schema of Used Tables"):
                        for table in used_tables:
                            st.subheader(f"Schema for Table: {table}")
                            schema = db.get_table_schema(table)
                            st.code(schema, language="sql")
            else:
                st.write("No result returned due to an error.")
        else:
            st.write("Please enter a question.")
