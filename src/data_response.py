import logging
import os

from abc import ABC, abstractmethod
import google.generativeai as genai
from dotenv import load_dotenv
load_dotenv('.env')


class Response(ABC):
    """
    Abstract class for response strategies.
    """

    @abstractmethod
    def get_response(self, matching_chunks, query) -> str:
        """
        Abstract method to get the response.
        """
        pass

class GeminiResponse(Response):
    """
    Gemini response strategy.
    """

    def get_response(self, matching_chunks, query) -> str:
        """Get the response from the Gemini model."""
        # Prepare context from chunks
        logging.info("Preparing context for response...")
        context = ""
        for chunk in matching_chunks:
            context += f"{chunk}\n\n"
        
        input_prompt = f"""
        Context (table metadata information):
        {context}
        
        Question: {query}
        
        Answer based only on the table information provided above:
        """
        # Set the API key
        genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
        # Generate response
        llm = genai.GenerativeModel('gemini-1.5-flash')
        logging.info("Generating response...")
        response = llm.generate_content(input_prompt)
        return response.text