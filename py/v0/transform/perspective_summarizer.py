from openai import OpenAI
from typing import List
from pydantic import BaseModel
import json

system_prompt_template = """
You are a research assistant and your job is to summarize articles for me in a manner which emphasizes information relevant to my interests. 
Below, I will provide the content of a document, and an explanation of why I am interested in the document.
I want you to summarize the document in a way that is interesting to me, and that is focused on the information I am interested in.
Your summary should be a maximum of 100 words and one paragraph.
Think step by step, and provide reasoning.

You response should be json, with the following fields:
- summary: str
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
Thanks to Charmander's quick thinking and courageous actions, the rangers were able to contain the blaze more effectively, minimizing damage to the forest and its ecosystem. "Charmander's bravery undoubtedly saved many lives today," said Ranger Jenny. "It's a reminder of how Pokémon and humans can work together in harmony to overcome natural disasters." Charmander, now hailed as a local hero, has inspired trainers and Pokémon alike, showcasing the true power of teamwork and resilience. Also, in an unprecedented event, Pokémon researchers have reported a surge in the Caterpie and Metapod populations within Viridian Forest. Over the past week, trainers and scientists alike have observed a remarkable increase, with an estimated 200% rise in the number of these Bug-type Pokémon. This phenomenon has intrigued the Pokémon research community, prompting a detailed investigation into the factors contributing to this sudden population boom.
Initial studies suggest that favorable weather conditions, combined with an abundance of their favorite food sources, have created an ideal environment for their rapid reproduction. The discovery has delighted trainers and bug enthusiasts, who are flocking to the forest in hopes of catching these Pokémon in their natural habitat. Local authorities, however, have issued guidelines to ensure that the ecosystem remains balanced, urging trainers to exercise restraint and respect the forest's delicate biodiversity.

System response:
```json
{
  "summary": "Brave Charmander turned hero by using its fire-control abilities to help contain a wildfire in Viridian Forest, creating safe pathways and guiding other Pokémon to safety. This courageous act minimized the forest damage and saved many lives, earning Charmander local hero status and showcasing true teamwork. "
,
  "reasoning": "The summary spotlights Charmander's heroism in mitigating a wildfire, which aligns directly with your interest in Charmander's actions and characteristics. The rest of the document, particularly the part about Caterpie and Metapod population changes, was omitted as it doesn't pertain to Pikachu or Charmander."
}
```


Example 2:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In the small town of Ember Grove, local authorities have issued a fire safety alert after multiple Charmander Pokémon were spotted near residential areas. Residents are advised to keep flammable materials secure and ensure their fire safety equipment is up to date. Charmanders, known for their flame-tipped tails, can inadvertently start fires if not carefully monitored.
Mayor Lucy Green reassured the community, stating, "Our top priority is the safety of our residents. We are working with Pokémon trainers and fire safety experts to manage the situation." Pokémon trainers from neighboring towns have volunteered to help relocate the Charmanders to safer habitats, ensuring the community's safety and the well-being of the Pokémon.
Separately, a wild Venusaur was spotted in downtown Tokyo today, causing a stir among locals and tourists alike. The massive Pokémon, known for its plant-covered back and powerful solar beam attack, appeared near a busy intersection during the afternoon rush hour. Witnesses report that the Venusaur seemed calm, merely basking in the sunlight before wandering into a nearby park. Pokémon trainers and experts from around the city quickly gathered, eager to catch a glimpse and study the rare occurrence. Authorities ensured public safety while Pokémon researchers began to hypothesize about what might have drawn Venusaur out of its natural habitat and into the bustling heart of the city.

System response:
```json
{
  "summary": "In Ember Grove, Charmanders near residential areas prompted a fire safety alert. Residents are advised to secure flammable items and check fire safety equipment. Pokémon trainers are aiding in relocating the Charmanders to ensure safety for both the residents and Pokémon.",
  "reasoning": "This summary focuses on the section about Charmanders near Ember Grove, aligning with your interest in Charmander. It omits the part about the Venusaur in Tokyo, as it is not relevant to your interests in Pikachu and Charmander."
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

class PerspectiveSummarizer:
  def __init__(self, openai: OpenAI):
    self.openai = openai

  def apply(self, docs: List[AnnotatedDoc]) -> List[AnnotatedDoc]:
    annotated_docs: List[AnnotatedDoc] = []
    
    for doc in docs:
      annotation = json.loads(doc.annotation)

      system_prompt = system_prompt_template
      user_prompt = user_prompt_template.format(document_content=doc, search_description=annotation['reasoning'])
      print(user_prompt)
      messages=[
        {"role": "system", "content": system_prompt}, 
        {"role": "user", "content": user_prompt}
      ]
      response = self.openai.chat.completions.create(
        messages=messages,
        model="gpt-4o",
      )
      print(response)
      content = response.choices[0].message.content
      if content:
        # Check if content is wrapped in a markdown code block
        if content.startswith("```json"):
          # Strip markdown code block syntax to extract JSON
          json_str = content[7:-3].strip()
        else:
          json_str = content

        annotated_docs.append(AnnotatedDoc(doc=doc.doc, annotation=json_str))

    return annotated_docs

  @staticmethod
  def from_env():
      openai = OpenAI()
      return PerspectiveSummarizer(openai)
