import json
from typing import List, Optional

from openai import OpenAI
from openai.types.chat.chat_completion_message_param import \
    ChatCompletionMessageParam

from ..llm_response_cache.llm_response_cache import LlmResponseCache


def create_completion(cache: LlmResponseCache, openai: OpenAI, messages: List[ChatCompletionMessageParam], model: str) -> str:
  request_str = json.dumps(messages)
  
  cached_response = cache.get_llm_response(request_str, model)
  content = cached_response
  if cached_response is None:
    response = openai.chat.completions.create(
      messages=messages,
      model=model,
    )
    content = response.choices[0].message.content  
  
  json_str = '{}'
  if content is None:
    raise ValueError("No content in response")

  # Check if content is wrapped in a markdown code block
  if content.startswith("```json"):
    # Strip markdown code block syntax to extract JSON
    json_str = content[7:-3].strip()
  else:
    try:
      json.loads(content)
      json_str = content
    except json.JSONDecodeError:
      raise ValueError(f"Failed to decode JSON: {content}")

  # Do this after we validate it.
  if cached_response is None:
    cache.insert_llm_response(request_str, model, content)

  return json_str