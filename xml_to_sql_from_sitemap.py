from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from ftplib import FTP
import os

def ftp_reach_xml():
    try:
        # Connect to FTP server and login
        ftp = FTP("ftpupload.net")
        ftp.login(user="if0_35194504", passwd="45SFkL7u3P3EtaR")

        # Change working folder
        ftp.cwd("htdocs")

        # Create local folder as "newssitemaps"
        os.makedirs("newssitemaps", exist_ok=True)

        # List all files and subdirectories in folder
        files = ftp.nlst()

        # Download all XML files
        for file in files:
            if file.endswith(".xml"):  # Select only XML files
                path = os.path.join("newssitemaps", file)  
                with open(path, "wb") as local_file:
                    ftp.retrbinary("RETR " + file, local_file.write)
        print("XML dosyalari indirildi.")

        # Close FTP connection
        ftp.quit()
    except Exception as e:
        print("Bir hata oluştu:", e)

def create_table():
    # Specify the folder where the XML files are located
    folder = "newssitemaps"
    all_data=[]
    checked_links=set()
    # List files in directory
    for file_name in os.listdir(folder):
        if file_name.endswith(".xml"):  # Select only XML files
            path = os.path.join(folder, file_name)
            # Assigns the "link" and "pubDate" data in the file to the all_data list and returns it to DataFrame
            with open(path, "r", encoding="utf-8") as xml_file:
                soup = BeautifulSoup(xml_file, "xml")
                for item in soup.find_all('item'):
                    link = item.find('link').text
                    # If sitemap's link is not already in checked links
                    if link not in checked_links:
                        pub_date_str = item.find('pubDate').text
                        date_obj = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
                        time = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                        all_data.append([link, time])
                        checked_links.add(link)
    print("Dataframe oluşturuldu")
    return pd.DataFrame(all_data, columns=["URL", "DATE"])

def save_to_database(table_name):
    # Create the database connection
    engine = create_engine("mysql+pymysql://remote:BIw883k8@212.31.2.93/monitor", connect_args={"charset": "utf8mb4"}, echo=False)

    existing_data = pd.read_sql_table(table_name, engine)
    # Compare new data with existing data
    new_data = df[~df['URL'].isin(existing_data['URL'])]
    #print(new_data)

    # Just add new data to database
    try:
        if not new_data.empty:
            new_data.to_sql(table_name, engine, if_exists="append", index=False)
            print("Veritabanı güncellendi.")
        else:
            print("Yeni veri yok, veritabanı güncellenmedi.")
    except Exception as e:
        print(f"Veritabanına yazma sırasında bir hata oluştu: {e}")


if __name__=="__main__":
    # Main process
    ftp_reach_xml()
    df= create_table()
    save_to_database("sitemap_urls")

