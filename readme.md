# Taiwan Assortative mating on Wealth Research 

Here is the repository of my ongoing bachelor thesis research on how wealth take an important role in Taiwan marriage market. 

The following code have different function, 

- clear_marr_data.R: This file clean the marriage data in Admin database first, include adding few variable we need for the future research.

- add_asset_marr_panel.R : This file add the asset data to our marriage data. which get the personal data from merge asset and attach them to each husband or wife for yearly pre-marriage asset and post-marriage asset

- marr_convert.R : This file aim to converting the person-base marriage into partners-base data. Each row would record each marriage, from both id, start year ,end year, etc.

- analysis.R : This file is the main file where I want to replicate one literature by our Taiwanese data first.
