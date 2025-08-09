import re

class PromptHandler:
    def set_default_prompt(self):
        return """You are a semantic analysis AI. Extract key information from the provided text.
Return ONLY a valid JSON object with these exact keys:
- key_information (list): Main points or themes extracted from the text
Document text:
\"\"\"
{chunk}
\"\"\"
"""

    def infer_type(self, prompt_text):
        lower_text = prompt_text.lower()
        if 'yes' in lower_text and 'no' in lower_text:
            return "boolean"
        if 'separated by' in lower_text or 'bulleted list' in lower_text or 'multiple' in lower_text or 'list' in lower_text:
            return "list"
        return "string"

    def generate_combined_prompt(self, prompts):
        combined = "You are an AI assistant that specialises in extracting structured parameters from free-text.\n"
        combined += "IMPORTANT: Extract STRICTLY from the provided document text ONLY. Do NOT use any external knowledge, assumptions, or previous contexts. If information is not in the text, use null or 'Not detected' as specified. Do not hallucinate or generalize.\n"
        combined += "Follow the following instructions for each parameter:\n\n"
        for pid, ptext in prompts.items():
            match = re.search(r'"(Parameter: .*? Output Format: .*?)"', ptext, re.DOTALL)
            block = match.group(1) if match else ptext
            combined += block + "\n\n"
        combined += "Ignore the individual Output Format and instead\n"
        combined += "Return ONLY a valid JSON object with these exact keys:\n"
        for pid, ptext in prompts.items():
            key = pid.replace(" ", "_").replace("/", "_").replace(":", "").replace("-", "_").lower()
            type_desc = self.infer_type(ptext)
            desc_start = ptext.find("Description:")
            desc_end = ptext.find("Output Format:")
            desc = ptext[desc_start + len("Description:"):desc_end].strip() if desc_start != -1 and desc_end != -1 else ""
            combined += f"- {key} ({type_desc}): {desc}\n"
        combined += "Use null for a parameter if the instruction says to output None or if not found in the text.\n"
        combined += "Document text:\n\"\"\"\n{chunk}\n\"\"\""
        return combined