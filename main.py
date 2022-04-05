import happybase
import declxml as xml


def connect():
    connection = happybase.Connection('127.0.0.1')
    #ONLY RUN THIS ONCE, IF YOU HAVEN'T SET UP THE DATABÆSE
    connection.create_table('foods', {"cf1": dict()}) 
    table = connection.table('foods')
    return table

    
def read_from_xml(table):
    with open('Food_Display_Table.xml', 'r') as f:
        data = f.read() 

    #Loads each field from the xml file
    processor = xml.dictionary('Food_Display_Table', [
        xml.array(xml.dictionary('Food_Display_Row', [
            xml.integer('Food_Code'),
            xml.string('Display_Name'),
            xml.floating_point('Portion_Default'),
            xml.floating_point('Portion_Amount'),
            xml.string('Portion_Display_Name'),
            xml.floating_point('Factor', required=False, default = 'none',  omit_empty=True),
            xml.floating_point('Increment'),
            xml.floating_point('Increment'),
            xml.floating_point('Multiplier'),
            xml.floating_point('Grains'),
            xml.floating_point('Whole_Grains'),
            xml.floating_point('Vegetables'),
            xml.floating_point('Orange_Vegetables'),
            xml.floating_point('Drkgreen_Vegetables'),
            xml.floating_point('Starchy_vegetables'),
            xml.floating_point('Other_Vegetables'),
            xml.floating_point('Fruits'),
            xml.floating_point('Milk'),
            xml.floating_point('Meats'),
            xml.floating_point('Soy'),
            xml.floating_point('Drybeans_Peas'),
            xml.floating_point('Solid_Fats'),
            xml.floating_point('Added_Sugars'),
            xml.floating_point('Alcohol'),
            xml.floating_point('Calories'),
            xml.floating_point('Saturated_Fats')
        ]), alias='foods')
    ])

    my_dict = xml.parse_from_string(processor, data)
    #Allows us to send in batches, which is pretty neat
    batcher = table.batch()


    #Accoring to the documentation, there should be an auto-incrementer. But i tried for an hour, and it doesn't work. So i made a scuffed version.
    number = 1
    for element in my_dict['foods']:
        column_string = str(f'cf1:{number}')
        string_element = str(element)
        batcher.put(b'row-key', {column_string: string_element})
        number = number + 1
    #Sends the batch
    batcher.send()

#Just wraps the file up
def run():
    table = connect()
    read_from_xml(table)



#Take a guess
run()

