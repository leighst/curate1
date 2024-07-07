import json
from enum import Enum
from typing import List

from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam

from ..llm_response_cache.llm_response_cache import (DbResponseCache,
                                                     LlmResponseCache)
from .middleware import create_completion
from .model import AnnotatedDoc

system_prompt_template_highly_relevant = """
You are a research assistant and your job is to search for articles relevant to my interests. 
Below, I will provide the content of a document, and a detailed description of the sort of documents I am looking for.
I want you to tell me whether the document is highly relevant to my interests, and if so, why it is relevant.
If it is not highly relevant, tell me why it is not relevant.
Think step by step, and provide reasoning.

You response should be json, with the following fields:
- relevant: bool
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
```json
{
  "relevant": true,
  "reasoning": "Based on your expressed interest in Pikachu and Charmander, this post would be highly relevant to you. The story centers on Charmander, one of the two Pokémon characters you are specifically interested in, and highlights a notable event involving his bravery and abilities. It aligns with your interest in news and writing about these characters, providing a narrative that delves into Charmander's actions and the impact he had, which would likely captivate and engage you. Although it doesn't mention Pikachu, it still falls within your specified interest in Charmander, making it a pertinent and appealing piece of content for you."
}
```


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
```json
{
  "relevant": false,
  "reasoning": "Based on the information you provided, this post would not be highly relevant to you. Your specific interest lies in Pikachu and Charmander, and my post did not mention either of these Pokémon. Therefore, it does not align with your stated preferences for content related to Pikachu and Charmander, whether it be news, history, or theories about these characters."
}
```
"""

system_prompt_template_maybe_relevant = """
You are a research assistant and your job is to search for articles relevant to my interests. 
Below, I will provide the content of a document, and a detailed description of the sort of documents I am looking for.
I want you to tell me whether the document is possibly relevant to my interests, and if so, why it might be relevant. If there is even a small chance that it is relevant, please include it. For this task it is better to err on the side of caution.
If it is not definitely not relevant, tell me why it is not relevant.
For example, if the article only mentions some topic which is closely related to the desired topic, it would be better to include it, so I can decide for myself.
Think step by step, and provide reasoning.

You response should be json, with the following fields:
- relevant: bool
- reasoning: str

Here's a few examples to get you started:

Example 1:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In a major policy shift, the Hoenn region has announced new regulations aimed at preserving its natural habitats and biodiversity. The new measures include stricter controls on industrial emissions, expanded protected areas for endangered species, and incentives for sustainable agriculture. Regional Governor Lana Rivers highlighted the importance of these actions in combating climate change and ensuring a healthier environment for future generations.
During the announcement, a technical glitch briefly interrupted the broadcast, causing the screen to flicker and display a random image of a Pikachu plush toy before resuming. The incident was quickly brushed off by officials, who assured the audience that it had no relevance to the important environmental policies being discussed.

System response:
```json
{
  "relevant": true,
  "reasoning": "The document mentions Pikachu, specifically an image of a Pikachu plush toy, which aligns with your interest in Pikachu. Even though the main content is about environmental policies in the Hoenn region, the brief mention of Pikachu means it could be worthwhile for you to review the document yourself to decide its relevance."
}
```


Example 2:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In a heartwarming tale from Lavender Town, a local Jigglypuff has become an overnight sensation for its incredible singing talent. Known affectionately as "Melody," this Jigglypuff has been performing nightly concerts in the town square, drawing crowds from far and wide. Melody's soothing lullabies have not only captivated the audience but also brought a sense of peace and tranquility to the usually bustling town.
Local shopkeeper and amateur musician, Tom Hartman, discovered Melody's talent when he heard her singing softly near his store. He began accompanying her on his guitar, and soon their impromptu performances became a nightly event. Residents describe Melody's voice as magical, with many reporting better sleep and reduced stress after attending the concerts. The town is now planning to host a special festival in honor of Melody, celebrating the joy and unity she has brought to their community.

System response:
```json
{
  "relevant": true,
  "reasoning": "The document is specifically about Jigglypuff and its singing talent in Lavender Town. It does not mention Pikachu or Charmander at all, nor does it provide any information or theories related to these two Pokémon characters. However, because Jigglypuff is a Pokemon, and because information relevant to certain, different Pokemon are highly relevant to you, it could be worthwhile for you to review the document yourself to decide its relevance."
}
```

Example 3:

User request:
SEARCH DESCRIPTION:
I am interested pokemon, but I am specifically interested in just two pokemon characters: Pikachu and Charmander.
Any news about Pikachu and Charmander is highly relevant to me.
Also any posts or writing about these two characters even if its not news, but just something about them, perhaps their history or some theory about what makes them tick.

DOCUMENT CONTENT:
In a groundbreaking medical advancement, researchers at the Goldenrod City Hospital have successfully developed a new treatment for a rare genetic disorder affecting thousands of people worldwide. The treatment, which involves a combination of gene therapy and personalized medicine, has shown remarkable results in clinical trials, offering hope to patients and their families who have long awaited a cure.
Dr. Maria Sanchez, the lead scientist on the project, explained that this breakthrough could revolutionize the field of genetic medicine. "This is a significant step forward in our ability to treat complex genetic conditions," she stated. The research team plans to publish their findings in a leading medical journal next month and is already preparing for the next phase of testing, which will bring this life-changing treatment one step closer to widespread availability.

System response:
```json
{
  "relevant": false,
  "reasoning": "The document is focused on a medical advancement related to gene therapy and personalized medicine, with no mention of Pokemon or the specific characters Pikachu and Charmander. It does not contain any content that is relevant to the interests specified in the search description."
}
```
"""

user_prompt_template = """
SEARCH DESCRIPTION:
{search_description}

DOCUMENT CONTENT:
{document_content}
"""

MODEL = "gpt-4o"

class Relevance(Enum):
  HIGHLY_RELEVANT = "highly_relevant"
  MAYBE_RELEVANT = "maybe_relevant"


class FilterSpec:
  def __init__(self, openai: OpenAI, cache: LlmResponseCache, relevance: Relevance):
    self.openai = openai
    self.cache = cache
    self.recall = relevance
    
    if self.recall == Relevance.MAYBE_RELEVANT:
      self.system_prompt = system_prompt_template_maybe_relevant
      self.model = "gpt-3.5-turbo"
      self.content_limit = 10000
    elif self.recall == Relevance.HIGHLY_RELEVANT:
      self.system_prompt = system_prompt_template_highly_relevant
      self.model = "gpt-4o"
      self.content_limit = 116000
    else:
      raise ValueError(f"Invalid relevance: {self.recall}")

    print(f"Using model: {self.model}, relevance: {self.recall}")

  def apply(self, docs: List[str], spec_file: str) -> List[AnnotatedDoc]:
    annotated_docs: List[AnnotatedDoc] = []

    spec = ""
    with open(spec_file, "r") as f:
      spec = f.read()
      
    for doc in docs:
      if len(doc) > self.content_limit:        
        json_str = json.dumps({"relevant": True, "reasoning": "The document is too long to process, but it might be relevant to you. Please review it yourself."})
        annotated_docs.append(AnnotatedDoc(doc=doc, annotation=json_str))  
        
      user_prompt = user_prompt_template.format(document_content=doc, search_description=spec)
      
      messages: List[ChatCompletionMessageParam] = [
        {"role": "system", "content": self.system_prompt}, 
        {"role": "user", "content": user_prompt}
      ]
      
      json_str = create_completion(self.cache, self.openai, messages, self.model)
      annotated_docs.append(AnnotatedDoc(doc=doc, annotation=json_str))

    return annotated_docs

  @staticmethod
  def from_env(relevance: Relevance):
      openai = OpenAI()
      cache = DbResponseCache.from_env()
      return FilterSpec(openai, cache, relevance)
