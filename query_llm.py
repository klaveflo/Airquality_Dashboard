import os
from google import genai

def ask_llm_about_peak(date_str, metric, country="ALL", user_query=None):
    """Query Gemini about air quality data. Uses user_query if provided, otherwise explains the date's peak."""
    if not os.environ.get("GEMINI_API_KEY"):
        return "AI feature requires a Gemini API key. See README for setup."

    try:
        client = genai.Client()
        location = f"in {country}" if country != "ALL" else "in Europe"

        if user_query and user_query.strip():
            prompt = (
                f"Context: The user is looking at air quality data ({metric}) "
                f"{location} on {date_str}.\n\n"
                f"Their question: {user_query}\n\n"
                f"Answer concisely in 2-3 sentences based on this context."
            )
        else:
            prompt = (
                f"Explain briefly in 2-3 sentences what event could cause "
                f"an air quality ({metric}) peak on {date_str} {location}."
            )

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"Error communicating with Gemini API: {str(e)}"
