import os
from quixstreams import Application, State
from openai import OpenAI

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(
    consumer_group=os.environ["groupname"],
    auto_create_topics=True, 
    auto_offset_reset="earliest",
    use_changelog_topics=False
)

openai_client = OpenAI(api_key=os.environ["openai_api_key"])
gptmodel=os.environ["gpt_model"]
input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)


def get_summary(row, model=gptmodel):
    completion = openai_client.chat.completions.create(
    model=model,
    messages=[
        {"role": "system", "content": "You are an expert at deciphering and summarizing long transcripts that contain unstructured dialog, often without correct punctuation."},
        {"role": "user", "content": f"The following transcript contains some fragmented dialog and cut off sentences but there are also some salient points in there. Please pick out the most salient points and summarize them using the present perfect verb tense. Do not include any extra commentary, just the summary.\n\nTRANSCRIPT:\n\n{row['chunks']}"}
    ]
    )

    return completion.choices[0].message.content

sdf = sdf.filter(lambda row: row["chunklen"] > 0)

# apply the result of the count_names function to the row
sdf["summary"] = sdf.apply(get_summary, stateful=False)

# print the row with this inline function
sdf = sdf.update(lambda row: print(row))

# publish the updated row to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)