import re


def parse_vtt(vtt_text: str) -> list[tuple[str, str, str]]:
    """Parse text from a VTT subtitle file and return a list of tuples with start time, end
    time, and text."""
    # Remove the VTT header and any styling information
    vtt_text = re.sub(r"WEBVTT.*?\n\n", "", vtt_text, flags=re.DOTALL)
    vtt_text = re.sub(r"<[^>]+>", "", vtt_text)  # Remove HTML tags

    # Split the text into lines and parse the timestamps and text
    lines = vtt_text.splitlines()
    parsed_lines = []
    for line in lines:
        if "-->" in line:
            start_time, end_time = line.split(" --> ")
            start_time = start_time.strip()
            end_time = end_time.strip()
        else:
            text = line.strip()
            if text:
                parsed_lines.append((start_time, end_time, text))

    return parsed_lines
