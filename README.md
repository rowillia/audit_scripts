# audit_scripts
Scripts to help with state audits

```
$ python parse_google_takeout.py --state "New York" --year 2019 --states-geojson ~/Downloads/gz_2010_us_040_00_500k.json --takeout-dir ~/Downloads/Takeout
Report for year 2019 in New York:
	Days Working in New York: 205
	Days Not Working in New York: 44
```

## Exporting Google Location History
1) Login into the Google associated with your Timeline account
2) Navigate to https://takeout.google.com/settings/takeout
3) Export your location history <img width="577" alt="Screen Shot 2020-11-12 at 9 42 54 PM" src="https://user-images.githubusercontent.com/808798/99022145-20918e00-2530-11eb-8c65-e90e4ceadb73.png">
4) Wait for an email stating your data is ready to download
