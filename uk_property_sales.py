import hashlib
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

"""
Properties labels assigned to the fields on the columns from the CSV data described on:
https://www.gov.uk/guidance/about-the-price-paid-data#download-option
"""
entryKeys = [
    "saleId",
    "price",
    "saleDate",
    "postcode",
    "propertyType",
    "newProperty",
    "contract",
    "address1",
    "address2",
    "street",
    "locality",
    "town",
    "district",
    "county",
    "saleType",
    "saleStatus"
]

"""
Fields labels chosen to define the description of a unique property, the intent is to use this
to group the data by this fields. 
"""

propertyKeys = [
    "postcode",
    "propertyType",
    "address1",
    "address2",
    "street",
    "locality",
    "town",
    "district"
    ]

"""
Fields to describe a Sale object, basically all the fields minus the ones to define a property
"""
saleKeys = list(set(entryKeys) - set(propertyKeys))

class MyOptions(PipelineOptions):
    """
    Class constructor inhereting options object to extend 
    the it an input and output command line argument.
    """

    @classmethod

    def _add_argparse_args(cls, parser):

        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/pp-complete.csv')

        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./output/properties.json')

class Split(beam.DoFn):
    """
    Pipeline tranformation object step to split the CSV line, into 
    a list of string removing double quotes and curly brakets.
    """
    def process(self, element):

        values = element.strip('"').replace('{', '').replace('}', '').split('","')

        return [
            dict(zip(entryKeys,values))
        ]

class Format(beam.DoFn):
    """
    Pipeline tranformation object step to format and constructs
    a unique property object with a list of associte sales with it.
    """
    def process(self, element):

        groupEntries = element[1] #Get the list of group elements from the grouped tuple second object.

        propertyValues = list(map(groupEntries[0].get, propertyKeys)) #Get property values from first item on list of grouped entries.
        propertyObject = dict(zip(propertyKeys,propertyValues)) # Build a property object based ond the labels an values. 

        saleObjectList = [] # Intitiate sales object list for a property.  

        for entry in groupEntries:

            saleValues = list(map(entry.get, saleKeys)) # Get sale value of of the entry element. 
            saleObject = dict(zip(saleKeys,saleValues)) # Build a sale object based on labes and values of the entry.
            saleObjectList.append(saleObject) # Append Sale object to the list.

        propertyString = "".join(propertyValues) # Create a string based on the unique values of the property object.
        propertyObject["id"] = hashlib.md5(propertyString.encode()).hexdigest() #Create a checksum of the unique property value and added as an unique id to the property object.
        propertyObject["sales"] = saleObjectList # Add sales object list belonging to the property to the propery object.

        return [
            propertyObject
        ]

with beam.Pipeline(options=MyOptions()) as p:
    """
    Run pipeline with following steps:
    1) Read CSV text file line by line.
    2) Split each lines using the Split object defined above, leaving a clean list of element values.
    3) Group all element with properties matching the value on propertyKeys.
    4) Format all the group sales by property above and format it into a single property object with a sale list.
    5) Write to file as line limited JSON.
    """
    (p 
    | beam.io.ReadFromText(p.options.input, skip_header_lines=1) 
    | beam.ParDo(Split())
    | beam.GroupBy(lambda s: tuple(map(s.get,propertyKeys)))
    | beam.ParDo(Format())
    | beam.io.WriteToText(p.options.output) 
    )