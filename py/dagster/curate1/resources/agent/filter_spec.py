import json
from typing import List

from openai import OpenAI
from pydantic import BaseModel

system_prompt_template = """
You are a research assistant and your job is to search for articles relevant to my interests. 
Below, I will provide the content of a document, and a detailed description of the sort of documents I am looking for.
I want you to tell me whether the document is highly relevant to my interests, and if so, why it is relevant.
If it is not highly relevant, tell me why it is not relevant.
Think step by step, and provide reasoning.

You response should be json, with the following fields:
- highly_relevant: bool
- reasoning: str

Here's a few examples to get you started:

Example 1:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In a remarkable turn of events, a brave Charmander became an unexpected hero when a wildfire broke out in Viridian Forest yesterday afternoon. Local Pokémon Rangers reported that the fire, likely caused by a lightning strike, spread rapidly through the dry underbrush. As panic set in among the forest's inhabitants, Charmander's innate fire-handling abilities proved invaluable. Witnesses say the young Fire-type Pokémon used its ember control to create safe pathways and guide other Pokémon to safety, effectively acting as a living firebreak.
Thanks to Charmander's quick thinking and courageous actions, the rangers were able to contain the blaze more effectively, minimizing damage to the forest and its ecosystem. "Charmander's bravery undoubtedly saved many lives today," said Ranger Jenny. "It's a reminder of how Pokémon and humans can work together in harmony to overcome natural disasters." Charmander, now hailed as a local hero, has inspired trainers and Pokémon alike, showcasing the true power of teamwork and resilience.

System response:
{
  "highly_relevant": true,
  "reasoning": "Based on your expressed interest in Pikachu and Charmander, this post would be highly relevant to you. The story centers on Charmander, one of the two Pokémon characters you are specifically interested in, and highlights a notable event involving his bravery and abilities. It aligns with your interest in news and writing about these characters, providing a narrative that delves into Charmander's actions and the impact he had, which would likely captivate and engage you. Although it doesn't mention Pikachu, it still falls within your specified interest in Charmander, making it a pertinent and appealing piece of content for you."
}


Example 2:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In an unprecedented event, Pokémon researchers have reported a surge in the Caterpie and Metapod populations within Viridian Forest. Over the past week, trainers and scientists alike have observed a remarkable increase, with an estimated 200% rise in the number of these Bug-type Pokémon. This phenomenon has intrigued the Pokémon research community, prompting a detailed investigation into the factors contributing to this sudden population boom.
Initial studies suggest that favorable weather conditions, combined with an abundance of their favorite food sources, have created an ideal environment for their rapid reproduction. The discovery has delighted trainers and bug enthusiasts, who are flocking to the forest in hopes of catching these Pokémon in their natural habitat. Local authorities, however, have issued guidelines to ensure that the ecosystem remains balanced, urging trainers to exercise restraint and respect the forest's delicate biodiversity.

System response:
{
  "highly_relevant": false,
  "reasoning": "Based on the information you provided, this post would not be highly relevant to you. Your specific interest lies in Pikachu and Charmander, and my post did not mention either of these Pokémon. Therefore, it does not align with your stated preferences for content related to Pikachu and Charmander, whether it be news, history, or theories about these characters."
}
"""

user_prompt_template = """
SEARCH DESCRIPTION:
{search_description}

DOCUMENT CONTENT:
{document_content}
"""

class AnnotatedDoc(BaseModel):
  doc: str
  annotation: str

class FilterSpec:
  def __init__(self, openai: OpenAI):
    self.openai = openai

  def apply(self, docs: List[str], spec_file: str) -> List[AnnotatedDoc]:
    annotated_docs: List[AnnotatedDoc] = []
    
    spec = ""
    with open(spec_file, "r") as f:
      spec = f.read()
      
    for doc in docs:
      system_prompt = system_prompt_template
      user_prompt = user_prompt_template.format(document_content=doc, search_description=spec)
      messages=[
        {"role": "system", "content": system_prompt}, 
        {"role": "user", "content": user_prompt}
      ]
      response = self.openai.chat.completions.create(
        messages=messages,
        model="gpt-4o",
      )
      content = response.choices[0].message.content
      if content:
        # Check if content is wrapped in a markdown code block
        if content.startswith("```json"):
          # Strip markdown code block syntax to extract JSON
          json_str = content[7:-3].strip()
        else:
          json_str = content

        annotated_docs.append(AnnotatedDoc(doc=doc, annotation=json_str))

    return annotated_docs

  @staticmethod
  def from_env():
      openai = OpenAI()
      return FilterSpec(openai)
