# -*- coding: utf-8 -*-
"""
Created on Sun May  4 08:43:44 2025

@author: Brent Thompson

https://python.langchain.com/docs/tutorials/sql_qa/
"""

#from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from langchain.chat_models import init_chat_model
from langchain_community.utilities import SQLDatabase
from typing_extensions import TypedDict, Annotated

# Testing to get the AI to read and only use the DB for responses

db = SQLDatabase.from_uri("sqlite:///C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/Complete_Dataset.db")

llm = init_chat_model("llama3.2:3b", model_provider="ollama")

human_input = "What module-ids are represented the best in this database"
user_prompt = f"Question: {human_input}"

system_message = """
Given an input question, create a syntactically correct {dialect} query to
run to help find the answer. Unless the user specifies in his question a
specific number of examples they wish to obtain, always limit your query to
at most {top_k} results. You can order the results by a relevant column to
return the most interesting examples in the database.

Never query for all the columns from a specific table, only ask for a the
few relevant columns given the question.

Pay attention to use only the column names that you can see in the schema
description. Be careful to not query for columns that do not exist. Also,
pay attention to which column is in which table.
Module Metadata is the most important table. 'module-id' links to all of the 
metadata tables. 

TABLE "module-metadata" (
	"module-id"	TEXT NOT NULL UNIQUE,
	"serial-number"	TEXT,
	"make"	TEXT,
	"model"	TEXT,
	"nameplate-pmp"	REAL,
	"nameplate-vmp"	REAL,
	"nameplate-imp"	REAL,
	"nameplate-voc"	REAL,
	"nameplate-isc"	REAL,
	"temperature-coefficient-voltage"	REAL,
	"temperature-coefficient-power"	REAL,
	"temperature-coefficient-current"	REAL,
	"module-packaging"	TEXT,
	"interconnection-scheme"	TEXT,
	"number-parallel-strings"	INTEGER,
	"cells-per-string"	INTEGER,
	"module-arc"	TEXT,
	"connector-type"	TEXT,
	"junction-box-locations"	TEXT,
	"number-junction-box"	INTEGER,
	"number-busbars"	TEXT,
	"cell-area"	TEXT,
	"module-area"	TEXT,
	"cell-technology"	TEXT,
	"wafer-doping-polarity"	TEXT,
	"wafer-crystallinity"	TEXT,
	"encapsulant"	TEXT,
	"backsheet"	TEXT,
	"frame-material"	TEXT,
	"x"	INTEGER,
	"y"	INTEGER,
	PRIMARY KEY("module-id")
);

Only use the following tables:
{table_info}
"""

query_prompt_template = ChatPromptTemplate(
    [("system", system_message), ("user", user_prompt)]
)

#for message in query_prompt_template.messages:
   # message.pretty_print()
    
    
class State(TypedDict):
    question: str
    query: str
    result: str
    answer: str

class QueryOutput(TypedDict):
    """Generated SQL query."""

    query: Annotated[str, ..., "Valid Raw SQL query."]

def write_query(state: State):
    """Generate SQL query to fetch information."""
    prompt = query_prompt_template.invoke(
        {
            "dialect": db.dialect,
            "top_k": 10,
            "table_info": db.get_table_info(),
            "input": state["question"],
        }
    )
    structured_llm = llm.with_structured_output(QueryOutput)
    result = structured_llm.invoke(prompt)
    return {"query": result["query"]}

write_query({"question": "How many modules are there?"})

