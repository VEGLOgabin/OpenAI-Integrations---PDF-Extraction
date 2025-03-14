from openai import OpenAI
import fitz  
import os
import pandas as pd
import json
import re
import logging
from key import setEnvVar


# Configure logging
logging.basicConfig(
    filename="pdf_scraper.log",
    level=logging.INFO,  # Log info, warnings, and errors
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def extract_text_from_pdf(pdf_path):
    logging.info(f"Extracting text from PDF: {pdf_path}")
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    logging.info(f"Successfully extracted text from PDF: {pdf_path}")
    return text

def setOpenAIKey():
    setEnvVar()
    logging.info("OpenAI API Key has been set.")

    
def at_PDF_Extractor(pdf_path):
    setOpenAIKey()
    logging.info(f"Starting data extraction for: {pdf_path}")

    pdf_text = extract_text_from_pdf(pdf_path)
    # pdf_text = extract_text_from_pdf()
    prompt = f""" 
    Extract product information from the provided text and return it in a structured format. The text contains technical specifications, product descriptions, and tabular data for multiple products. Your goal is to identify and extract the following fields for each product:

    **mfr name: The name of the manufacturer (e.g., "Baker")**,

    **model name: The model name or number of the product. **,

    **mfr number: The number of the product (e.g., "SG404", "SG504", "SG604")**,

    **unit cost: The cost of the product (usually in USD)**,

    **product description: A brief description of the product (e.g., "Class II, Type A2 Biosafety Cabinet")**,

    **amps: The amperage rating of the product (e.g., "20A")**,

    **volts: The voltage rating of the product (e.g., "100V")**,

    **watts: The wattage rating of the product. **,

    **phase: The phase type (e.g., "1Ø")**,

    **hertz: The frequency rating (e.g., "50/60 Hz")**.

    **plug_type: The type of plug.**,

    **btu: The BTU rating of the product (e.g., "1,434 BTU")**,

    **dissipation_type: The type of heat dissipation (e.g., "Air")**,

    **ship_weight: The shipping weight of the product (e.g., "712 lbs")**,

    **weight: The weight of the product (e.g., "582 lbs")**,

    **depth: The depth of the product in inches **,

    **height: The height of the product in inches (e.g., "14 in (35.6 cm)")**,

    **width: The width of the product in inches (e.g., "42 in (106.6 cm)")**,

    **Notes: Any additional notes or comments about the product.**.

    **Instructions:**

    **Parse the text and identify all products. Each product's information may be spread across multiple lines, tables, or sections**,

    **For each product, extract the values for the fields listed above. If a field is not mentioned in the text, return "" for that field**,

    **Pay special attention to tabular data, as it often contains key specifications (e.g., dimensions, weights, electrical ratings)**,

    **Return the data as a json data, so each dictionary should represent one product inside a list, with keys as the field names and values as the extracted data.**,

    **Let all product with mfr number equal to NAN or empty**,

    **The output should be a json data of products found**,

    **If any value is not available, return ""**,

    **If any field value is not available in the text, never put the example given value by defaut put only ""**,

    **In the case you have multiple values for a field for one product, keep the first one**,

    **Scrape,data from : {pdf_text}**

    """

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) 
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": "Act as an unstructured data scraping expect"
            },
            {
                "role": "user",
                "content": prompt
            },
        ],
        max_tokens = 1500,
        temperature = 0.0
    )

    response_text = completion.choices[0].message.content.strip()

    # print(response_text)

    json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
    if json_match:
        json_data = json_match.group(0)
        try:
            json_data = json.loads(json_data)
            logging.info(f"Successfully extracted structured data from: {pdf_path}")
            return json_data
        except json.JSONDecodeError:
            logging.error(f"Error: Unable to parse JSON response from: {pdf_path}")
            return None
    else:
        logging.error(f"Error: No JSON found in response for: {pdf_path}")
        return None



def reformat_extracted_data(json_data):
    logging.info("Starting data reformatting...")
    
    fields_to_clean = ["width", "height"]

    for item in json_data:

        if ":" in item["height"]:
            item["height"] = item["height"].split(":")[-1]
        if ":" in item["plug_type"]:
            item["plug_type"] = item["plug_type"].split(":")[-1]
    logging.info("Data reformatting completed.")
    return json_data



def save_scraped_data_to_excel(scraped_data, output_filename, columns):
    try:
        logging.info(f"Saving data to Excel file: {output_filename}")
        
        df = pd.DataFrame(scraped_data)
        for col in columns:
            if col not in df.columns:
                df[col] = "" 
        df = df[columns]
        df.to_excel(output_filename, index=False)

        logging.info(f"Successfully saved data to: {output_filename}")
    except Exception as e:
        logging.error(f"Error saving to Excel: {e}")



def main():
    pdf_paths = ["AT/SterilGARD-SGX04-Product-Specifications-RevE.pdf", "AT/2020 ProCuity Spec Sheet JB Mkt Lit 2077 07 OCT 2020 REV C 1.pdf"]
    columns = [
        'mfr website', 'mfr name', 'model name', 'mfr number', 'unit cost',
        'product description', 'amps', 'volts', 'watts', 'phase', 'hertz',
        'plug_type', 'emergency_power Required (Y/N)',
        'dedicated_circuit Required (Y/N)', 'tech_conect Required', 'btu ',
        'dissipation_type', 'water_cold Required (Y/N)',
        'water_hot  Required (Y/N)', 'drain Required (Y/N)',
        'water_treated (Y/N)', 'steam  Required(Y/N)', 'vent  Required (Y/N)',
        'vacuum Required (Y/N)', 'ship_weight', 'weight', 'depth', 'height',
        'width', 'ada compliant (Y/N)', 'green certification? (Y/N)',
        'antimicrobial coating (Y/N)', 'Specification Sheet (pdf)',
        'Brochure (pdf)', 'Manual/IFU (pdf)', 'Product URL', 'CAD (dwg)',
        'REVIT (rfa)', 'Seismic document', 'Product Image (jpg)', 'Notes'
    ]
    
    for path in pdf_paths:
        output_sheetname = str(path.split("/")[-1]).replace(".pdf", "") + "_.xlsx"
        logging.info(f"Processing file: {path}")
        data = at_PDF_Extractor(path)
        data = reformat_extracted_data(data)
        if data:
            save_scraped_data_to_excel(data, output_filename=output_sheetname, columns=columns)
        else:
            logging.warning(f"No data extracted for {path}")

    logging.info("All PDF processing completed.")

if __name__ == "__main__":
    main()
