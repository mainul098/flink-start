## Key Features for Testing Watermarks:

1. Out-of-order events: Events are not in chronological order - they jump around in time to simulate real-world scenarios where events arrive late.
2. Creative usernames: Including names like alice_wonderland, bob_builder, charlie_chocolate, leonardo_davinci, marie_curie, etc.
3. Diverse activities: All four activity types - register, login, click, logout
4. Time range designed for 1-minute watermarks:
   •  Events span from 13:14:30Z to 13:28:45Z (about 14+ minutes)
   •  Some events are more than 1 minute late (e.g., napoleon_bonaparte registers at 13:14:30Z but appears later in the stream)
   •  This will cause some events to be discarded when using a 1-minute watermark

Events that will likely be discarded with 1-minute watermark:
•  Events from napoleon_bonaparte (13:14:30 - 13:16:00) will be discarded when watermark advances beyond 13:17:00
•  Events from leonardo_davinci (13:16:00 - 13:17:30) will be discarded when watermark advances beyond 13:18:30
•  Several other early events that appear late in the stream
