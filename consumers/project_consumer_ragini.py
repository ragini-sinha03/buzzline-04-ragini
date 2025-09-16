import json
from collections import defaultdict
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Set up Kafka consumer (update topic name if needed)
consumer = KafkaConsumer(
    'buzzline_json',  # Replace with your topic name if different
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

author_counts = defaultdict(int)
fig_bar, ax_bar = plt.subplots()
fig_line, ax_line = plt.subplots()
plt.ion()

def update_charts():
    authors = list(author_counts.keys())
    counts = list(author_counts.values())
    # Bar chart
    ax_bar.clear()
    ax_bar.bar(authors, counts, color="skyblue")
    ax_bar.set_xlabel("Authors")
    ax_bar.set_ylabel("Message Counts")
    ax_bar.set_title("Real-Time Author Message Counts (Bar Chart)")
    ax_bar.set_xticklabels(authors, rotation=45, ha="right")
    # Line chart
    ax_line.clear()
    ax_line.plot(authors, counts, marker='o', color="blue")
    ax_line.set_xlabel("Authors")
    ax_line.set_ylabel("Message Counts")
    ax_line.set_title("Real-Time Author Message Counts (Line Chart)")
    fig_bar.tight_layout()
    fig_line.tight_layout()
    fig_bar.canvas.draw()
    fig_line.canvas.draw()
    plt.pause(0.01)

for message in consumer:
    author = message.value.get("author", "unknown")
    author_counts[author] += 1
    update_charts()

plt.ioff()
plt.show()
