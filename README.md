Code base related to - [LayerZero Labs Sybil Report Issue #58](https://github.com/LayerZero-Labs/sybil-report/issues/58)

These scripts are not rocket science. They might not look pretty or be well-organized, but they get the job done. The methodology here reduces the risk of mistakenly including real people in the data close to 0.

## data_mining.py

This part of the project collects data and saves it into a CSV file.

`wallets.csv`

This CSV file lists the wallet addresses that need information gathering. Collecting these addresses is a separate task, which can be done using Dune analytics. In my case, around 430,000 wallet addresses were gathered that had interacted with at least two popular airdrop farms and had some activity on Layer 0 protocols.

## clustering.py

Here’s where all the magic happens.

### Clustering Parameters:

- **date_tolerance = 4**: Tolerance for differences in days in protocol usage
- **date_range_tolerance = timedelta(days=4)**: Tolerance for differences in activation date
- **volume_tolerance = 0.30**: # Percentage tolerance for volume differences in a certain cluster in a certain protocol
- **final_clusters = [cluster for cluster in all_clusters if len(cluster) > 20]** - number of addreses in cluster, 20 in this case 

How flexible these settings are affects the likelihood of accidentally including people who shouldn't be grouped together.

After initially sorting the data into clusters, additional functions are implemented to ensure the results are as accurate as possible and to clean up data. 

For those who want to run `clustering.py` without the data gathering step, a `sample_data.csv` file is included, containing 10,000 addresses.

## Results

Initially, 40,000 addresses were clustered, out of which 30,000 were flagged by the initial filtering by the LayerZero team.

The remaining addresses are included in `report.csv`, alongside the flagged addresses, because they can share the same cluster.
