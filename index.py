import pandas as pd
import numpy as np

# Create dummy data
data = {
    "order_id": [101, 102, 103, 101, 104, 105, 102, 106, 107, 103],  # Repeated order IDs
    "price": np.random.randint(100, 1000, size=10)  # Random prices between 100 and 1000
}
#101 102 103 104 105 106 107
# Create DataFrame
df = pd.DataFrame(data)

# Display DataFrame
print(df)

avg_order_value = df.groupby("order_id")["price"].sum().mean()

ttt = df.groupby("order_id")["price"].sum()

print(ttt, end='\n')
print(avg_order_value)


