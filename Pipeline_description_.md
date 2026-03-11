## CONNECTION FROM ALDS GEN 2 USING STORAGE ACCOUNT AND CONTAINERS : USING THE GET META DATA ACTIVITIES
<img width="1104" height="738" alt="image" src="https://github.com/user-attachments/assets/0e4907db-6cdc-4876-b425-d24ad233ea51" />

## USNG FOR EACH ACTIVITIES TO USE DIFFERENTE JSON ENTITIES (DATA FROM CONTAINERS) : @activity('Get Metadata1').output.childItems
“Take the list of files/folders from Get Metadata and iterate through them one by one.”
Sequential : - Files are processed one at a time, in order

<img width="1861" height="910" alt="image" src="https://github.com/user-attachments/assets/695c455a-023a-4c8d-90a1-8696bd3bb960" />


INSIDE THE FOREACH WE PLACE THE COPY DATA ACTIVITY : For each file in the folder using @item() with the name property of that element @item().name, run a copy operation.


<img width="1160" height="714" alt="image" src="https://github.com/user-attachments/assets/cd4e737a-ca7a-4835-9ea4-b9d773c71442" />
