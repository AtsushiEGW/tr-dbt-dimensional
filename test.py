import pandas as pd


df = pd.read_csv('raw_data/orig/modified-spotify-dataset.csv')


print(df.shape)

ix = [
    {'start': 0, 'end': 5000},
    {'start': 4500, 'end': 9500},
    {'start': 9000, 'end': 14000},
    {'start': 13500, 'end': 18500},
    {'start': 18000, 'end': 23000},
    {'start': 22500, 'end': 27500},
    {'start': 27000, 'end': 32000},
    {'start': 31500, 'end': 36500},
    {'start': 36000, 'end': 41000},
    {'start': 40500, 'end': 45500},
    {'start': 45000, 'end': 50000},
    {'start': 49500, 'end': 54500},
    {'start': 54000, 'end': 59000},
    {'start': 58500, 'end': 63500},
    {'start': 63000, 'end': 68000},
    {'start': 67500, 'end': 72500},
    {'start': 72000, 'end': 77000},
    {'start': 76500, 'end': 81500},
    {'start': 81000, 'end': 86000},
    {'start': 85500, 'end': 90500},
    {'start': 90000, 'end': 95000},
    {'start': 94500, 'end': 99500},
    {'start': 99000, 'end': 104000},
    {'start': 103500, 'end': 108500},
    {'start': 108000, 'end': 113000},
    {'start': 112500, 'end': 113998}
]


for i, d in enumerate(ix):
    df_part = df.iloc[d['start']:d['end']]
    filename = f'raw_data/spotify/spotify-dataset-part-{i+1}.csv'
    df_part.to_csv(filename, index=False)
    print(f'Saved {filename} with shape {df_part.shape}')