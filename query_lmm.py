import os
from google import genai

def ask_lmm_about_peak(date_str, metric, context_data=None):
    """
    Sends a query to an LMM asking for an explanation of a peak on a given date.
    Returns a 1-2 sentence response.
    """
    if not os.environ.get("GEMINI_API_KEY"):
        return "Error: GEMINI_API_KEY environment variable not found. Please set your API key to use the AI feature."
        
    try:
        # The client automatically picks up the GEMINI_API_KEY environment variable
        client = genai.Client()
        
        prompt = (
            f"Explain briefly in only 1-2 sentences what event could cause there to be an air quality ({metric}) "
            f"peak on {date_str} in Europe. Keep your response short and concise."
        )
        
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"Error communicating with Gemini API: {str(e)}"
