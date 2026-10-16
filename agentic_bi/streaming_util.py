"""
Streaming generator utility for FreshFlow Agentic BI
Enables real-time typewriter / SSE streaming in Streamlit (st.write_stream)
"""

import re
import time
from typing import Generator

def stream_response_generator(full_text: str, delay: float = 0.012) -> Generator[str, None, None]:
    """
    Yields words / punctuation chunks with a natural typewriter cadence.
    Directly compatible with Streamlit st.write_stream().
    """
    words = re.split(r"(\s+)", full_text)
    for w in words:
        yield w
        if w.strip():
            time.sleep(delay)
