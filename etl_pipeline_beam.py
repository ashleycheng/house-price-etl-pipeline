from apache_beam.options.pipeline_options import PipelineOptions
from dateutil.parser import parse
import apache_beam as beam
import argparse
import pandas as pd
import csv
import io


def create_dataframe(readable_file):
    '''This function read a csv file into a Pandas dataframe, and filter data
    and columns related to land transactions.
    '''
    # Read a csv file
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    csv_dict = csv.DictReader(io.TextIOWrapper(gcs_file))

    # Create a DataFrame
    dataFrame = pd.DataFrame(csv_dict)
    dataFrame = dataFrame.drop(labels=0, axis=0)  # Remove the row of English header
    dataFrame.columns = dataFrame.columns.str.replace('\ufeff', '')  # Remove the hidden character
    dataFrame = dataFrame[dataFrame.交易標的 == '土地']  # Filter land transaction data
    # Select columns related to land transactions
    dataFrame = dataFrame[['鄉鎮市區', '交易標的', '土地位置建物門牌', '土地移轉總面積平方公尺', '交易年月日',
                           '總價元', '單價元平方公尺']]

    # Use the alphabet in the file name to map the city or county name
    # For example, when the file name is "101S4_g_lvr_land_a.csv", the alphabet
    # used to map the name of city of country in the file name is "g"
    city_code = {
        'a': '台北市', 'b': '台中市', 'c': '基隆市', 'd': '台南市', 'e': '高雄市', 'f': '新北市',
        'g': '宜蘭縣', 'h': '桃園縣', 'j': '新竹縣', 'k': '苗栗縣', 'l': '臺中縣', 'm': '南投縣',
        'n': '彰化縣', 'p': '雲林縣', 'q': '嘉義縣', 'r': '臺南縣', 's': '高雄縣', 't': '屏東縣',
        'u': '花蓮縣', 'v': '臺東縣', 'x': '澎湖縣', 'y': '陽明山', 'w': '金門縣', 'z': '連江縣',
        'i': '嘉義市', 'o': '新竹市'}
    filename = readable_file.split('/')[-1]
    city = city_code[filename[6]]
    dataFrame['縣市'] = city

    # Change Chinese column names to English (Bigquery doesn't support Chinese column names)
    dataFrame = dataFrame.rename(
        {'縣市': 'city', '鄉鎮市區': 'township_dist', '交易標的': 'transaction_sign',
         '土地位置建物門牌': 'position', '土地移轉總面積平方公尺': 'land_area_m2',
         '交易年月日': 'transaction_date', '總價元': 'total_price',
         '單價元平方公尺': 'unit_price_m2'}, axis='columns')
    yield dataFrame


class land_section(beam.DoFn):
    '''This function add a new column "section" by extracting
    section(地段) in the posion column.

    Example posion: '大湖段572地號'
    Returns: '大湖段'
    '''
    def process(self, element):
        element['section'] = element['position'].str.extract(r'^(.*段)')
        yield element


class date_format(beam.DoFn):
    '''This function change the transaction_date format, and check if the
    date is valid. Then filter data with valid date. Note that if the date
    in date type field is invalid, an error will occur when writing data into Bigquery.

    Example transaction_date: '1100718'
    Returns: '2021-07-18'
    '''
    def process(self, element):
        # check if the date is valid
        def is_valid_date(date):
            if date['transaction_date']:
                try:
                    parse(date['transaction_date'])
                    return 'True'
                except:
                    return 'False'
            return 'False'
        element['year'] = (element['transaction_date'].str[:-4].astype(int)+1911).apply(str)
        element['month'] = element['transaction_date'].str[-4:-2]
        element['day'] = element['transaction_date'].str[-2:]
        element['transaction_date'] = element[['year', 'month', 'day']].agg('-'.join, axis=1)
        element = element.drop(['year', 'month', 'day'], axis=1)

        element['check_date'] = element.apply(lambda x: is_valid_date(x), axis=1)
        element = element[element.check_date == 'True']
        element = element.drop(['check_date'], axis=1)
        yield element


class data_clean(beam.DoFn):
    '''This function check if the value of "unit_price_m2" column
    is 0, calculate the unit price per suare meter for it.
    '''
    def process(self, element):
        element['land_area_m2'] = element.land_area_m2.astype(float)
        element['total_price'] = element.total_price.astype(int)
        element['unit_price_m2'] = pd.to_numeric(element['unit_price_m2'], errors='coerce')
        element.loc[element['unit_price_m2'] == 0, 'unit_price_m2'] = element['total_price'] / element['land_area_m2']
        yield element


class price_m2_to_ping(beam.DoFn):
    '''This function convert the unit of area from square meter to ping.
    '''
    def process(self, element):
        element['land_area_m2'] = element['land_area_m2'] / 3.30579
        element['unit_price_m2'] = element['unit_price_m2'] * 3.30579
        element = element.rename({'unit_price_m2': 'unit_price_ping', 'land_area_m2': 'land_area_ping'}, axis=1)
        element = element.round(decimals=2)
        yield element


def concat_df(df_list):
    '''This function is able to concatenate dataframes'''
    merged = pd.concat(df_list)
    return merged


def parse_method(string_input):
    '''This function convert a dataframe to a dictionary
    which can be loaded into BigQuery.'''
    return(string_input.to_dict(orient='records'))


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        default='gs://BUCKET_NAME/land_data/*a.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        # To specify a Bigqeury table with a string
                        default='project_id:dataset_id.table_id')

    # Use a string to specify the table schema: "fieldName:fieldType"
    table_schema = '''city:STRING, township_dist:STRING, transaction_sign:STRING, position:STRING, section:STRING, land_area_ping:FLOAT, transaction_date:DATE, total_price:INTEGER, unit_price_ping:FLOAT'''
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pipeline:
        # get a list of files that match a input pattern
        readable_files = (
          pipeline
          | beam.io.fileio.MatchFiles(known_args.input)
          | beam.io.fileio.ReadMatches()
          | beam.Reshuffle())

        files_and_contents = (
          readable_files
            | 'Get the File URLs' >> beam.Map(lambda x: (x.metadata.path))
            | 'Read Files' >> beam.FlatMap(create_dataframe)
            | 'Add the Section Column' >> beam.ParDo(land_section())
            | 'Change the Date Format' >> beam.ParDo(date_format())
            | 'Clean Data' >> beam.ParDo(data_clean())
            | 'Convert the unit of area' >> beam.ParDo(price_m2_to_ping())
            | 'Combine Dataframes to List' >> beam.combiners.ToList()
            | 'Concatenate Dataframes' >> beam.Map(concat_df)
            | 'String To BigQuery Row' >> beam.FlatMap(parse_method)
            | 'Write To BigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                known_args.output,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )
        )


if __name__ == "__main__":
    run()
