# APACHE BEAM DATA PIPE EXAMPLE WITH UK PROPERTY PRICE DATA

This is an example execise for an Apache Beam pipeline digesting data from:
https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

The data can be downloaded from monthly/yearly basis or a full CSV dataset since 1st January 1995:
http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv

## CSV DATA COLUMN HEADERS LABELS MAPPING
Full definition on: https://www.gov.uk/guidance/about-the-price-paid-data#download-option

| Field Label  | Short Description                 | Type [Possible values] |
| ------------ | --------------------------------- | ---------------------- |
| saleId       | Transaction unique identifier     | String                 |             
| price        | Price                             | String Numeric         |     
| saleDate     | Date of Transfer                  | String Date            | 
| postcode     | Postcode                          | String UK Postcode     |         
| propertyType | Property Type                     | String [D,S,T,F,O]     |         
| newProperty  | Old/New                           | String [O,N]           |     
| contract     | Duration                          | String [F,L]           |     
| address1     | PAON                              | String                 |
| address2     | SAON                              | String                 |              
| street       | Street                            | String                 |              
| locality     | Locality                          | String                 |              
| town         | Town/City                         | String                 |              
| district     | District                          | String                 |              
| county       | County                            | String                 |              
| saleType     | PPD Category Type                 | String [A,B]           |     
| saleStatus   | Record Status - monthly file only | String [A,C,D]         |     

## DATA TRANSFORMATION INPUT VS OUTPUT
The main data tranformation is explained on the code and in the pipeline chain, but basically the script looks for all the reapeted sales that belong
to the same property using a groupby based on a collection of feild labels. The list of fields that can make a property can be changed if the definition is not correct there are some assumption that would make this logic fail, if the data entry of the same property is been typed differntly the grouping would fail to pick up on that as is basically string matching a list of fields, so for example address1: "First Street" can be typed: "First st", "1st st", "1st st", etc... there would be need of a fuzzy matching logic for the the exact word maybe using Cosine Similarity or another technique.

This exercise was using an exact matching a try to leverage Apache Beam function like groupBy to process data quicker and also process a large dataset 4.3GB of data approximatly.

### RAW DATA INPUT
This is and example of CSV data format typically found on the https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads datsets, observe that the two first entries would belong to the same property:

```csv
"{XXXXYYY1}","50000","1995-07-07","ML1 11A","D","N","F","01","","First street","CITY CENTER","MANCHESTER","MANCHESTER","MANCHESTER","A","A"
"{XXXXYYY2}","70000","2002-06-03","ML1 11A","D","N","F","01","","First street","CITY CENTER","MANCHESTER","MANCHESTER","MANCHESTER","A","A"
"{XXXXYYY3}","90000","1998-01-13","SK1 11A","T","N","F","999","","Coronation street","STOCKPORT","STOCKPORT","MANCHESTER","MANCHESTER","A","A"
```

### PROCESSED DATA OUTPUT
The output would be in a new line delimited JSON (not coma), and the property object would include the unique fields that define the property and unique id based on the checksum from the property field values with the intent to use checsum comparison if needed in long term storage, and each property would have a list of sales objects associated with it.

```json
{
    "postcode": "ML1 11A",
    "propertyType": "D", 
    "address1": "01", 
    "address2": "", 
    "street": "First street", 
    "locality": "CITY CENTER", 
    "town": "MANCHESTER", 
    "district": "MANCHESTER", 
    "county": "MANCHESTER", 
    "id": "aaadddfff11111", 
    "sales": 
    [
        {
            "saleId": "XXXXYYY1", 
            "saleDate": "1995-07-07", 
            "newProperty": "N", 
            "contract": "F", 
            "price": "50000", 
            "saleType": "A", 
            "saleStatus": "A"
        }, 
        {
            "saleId": "XXXXYYY2", 
            "saleDate": "2002-06-03", 
            "newProperty": "N", 
            "contract": "F", 
            "price": "70000", 
            "saleType": "A", 
            "saleStatus": "A"
        }
    ]   
}\n
{
    "postcode": "SK1 11A", 
    "propertyType": "T", 
    "address1": "999", 
    "address2": "", 
    "street": "Coronation street", 
    "locality": "STOCKPORT", 
    "town": "STOCKPORT", 
    "district": "MANCHESTER", 
    "county": "MANCHESTER", 
    "id": "aaadddfff222222", 
    "sales": 
        [
            {
                "saleId": "XXXXYYY3", 
                "saleDate": "1995-01-05", 
                "newProperty": "Y", 
                "contract": "F", 
                "price": "90000", 
                "saleType": "A", 
                "saleStatus": "A"
            }
        ]
}
```

## APACHE BEAM INSTALLATION, AND RUN INSTRUCTIONS ##
1) Using python install the requirements by running the command:
```bash
pip install requirements.txt
```
2) Run script on direct runner, showing default option values:
```bash
python uk_property_sales.py --input ./data/pp-complete.csv --output ./output/properties.json
```
### Requirements 

---
__Plattform:__

+ python3
+ python3-pip
+ python3-setuptools

__Python specific requirments:__

+ apache-beam
---

## Support Information. ##

* Author: Daniel Hung.
* Location: Manchester UK.
* email: danihuso@hotmal.com