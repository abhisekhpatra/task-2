import requests
import csv



class TFR:


    def get_forms(self,API_KEY):

        # Fetching a List of All  Typeforms
        api_url="https://api.typeform.com/v1/forms?key="+API_KEY

        # The request api returns a form object which can be queried to get the form details
        response=requests.get(api_url)
        resposne_status=response.status_code

        if resposne_status == 404:
             print("404 Error")
        elif resposne_status == 403:
            print("403 ERROR")

        else: return response.json();



    def get_data(self,API_KEY,id):
        # Fetching a List of All  Typeforms
        api_url="https://api.typeform.com/v1/form/"+id+"?key="+API_KEY
        # The request api returns a form object which can be queried to get the form responses and questions
        response=requests.get(api_url)
        resposne_status=response.status_code
        if resposne_status == 404:
             print("404 Error")
        elif resposne_status == 403:
             print("403 ERROR")

        else: json=response.json()

        # get_headers is used to extract the headers for our CSV. The json data extracted from response is passed to it as an argument.
        # The function returns an array of headers to be used in the CSV.
        headers=self.get_headers(json)

        # open a file for writing using the csv library
        answers = open('Data.csv', 'wb')
        csvwriter = csv.writer(answers)
        csvwriter.writerow(headers)

        # The returned json is queried to extract responses.
        response_data=json["responses"]

        # We loop over each response and query for answers.
        for response in response_data:
            # answers in each response is stored in row variable
             row=response["answers"]
            # if row is blank i.e we do not have any response, then we do not insert anything into the csv
             if len(row)==0: continue
            # csvrow is an array that stores the content that will be inserted into the CSV
             csvrow=[]
            # for each header value we query the response and store the values in csv
             for j in range(len(headers)):
                 # for each header value if we do not have any response then store blank values in CSV
                 if(headers[j] in row.keys()): csvrow.append(row[headers[j]])
                 else: csvrow.append("")
             csvwriter.writerow(csvrow)

        answers.close()
        print("Data Transfered and saved in /src/Data.csv")






    def get_headers(self,data):
        # The json data is queried to extract the "question" used in the form.
        questions=data["questions"]

        header=[]
        # We loop over each question to extract the id and store it into the header array.
        for q in questions:
            str=q["id"]
            # print(str)
            header.append(str)

        return header




