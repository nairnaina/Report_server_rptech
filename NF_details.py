import streamlit as st
import pyodbc
import pandas as pd

st.set_page_config(layout="wide")

# Initialize connection object
conn = None
# Database connection
try:
    connection_string = (
        "DRIVER={SQL Server Native Client 11.0};"
        "SERVER=192.168.100.1;"
        "DATABASE=RashiPortal_Live;"
        "UID=naina;"
        "PWD=naina@1202;"
    )
    conn = pyodbc.connect(connection_string)
except Exception as e:
    st.error(f"Failed to connect to the database: {e}")

# Main title and subtitle
st.title(" Detailed Notification Process Summary")
st.subheader("Notification Details")

#single input 
#notification_no=st.text_input("Enter Notification Number")

# multiple input
notification_no = st.text_area("Enter Notification Numbers (one per line):")

if notification_no:
    with st.spinner("Fetching data..."):
        try:
            # Split input into a list using newline(\n) as the delimiter
            notification_nos = [num.strip() for num in notification_no.split("\n") if num.strip()]
            

            if start_date and end_date and start_date > end_date:
                st.warning("Start Date cannot be after End Date.")

            # Prepare query condition
            placeholders = ",".join("?" for _ in notification_nos)
#queries
            # Query 1: Inward details
            query1 = f"""
            SELECT 
                BranchMaster.BranchName AS "Branch Name",
                CAST(n.NotificationNo AS VARCHAR) AS "Notification No",
                Notification.CustomerCode AS "Customer Code",
                CustomerMaster.CustomerName AS "Customer Name",
                CustomerMaster.PhoneNo AS "Phone No.",
                CustomerMaster.MobileNo AS "Mobile No.",
                CustomerMaster.EmailId AS "Email Id",
                CustomerMaster.City AS "City",
                Notification.CustomerChallanNo AS "Customer Challan No.",
                Notification.CustomerInvoiceNo AS "Customer Invoice No.",
                Notification.ChallanNo AS "Challan No.",
                Notification.ProductCode AS "Product Code",
                Notification.ProductSerialNo AS "Product Serial NO.",
                Notification.WarrantyDate AS "Warranty Date",
                Notification.Remarks,
                Notification.Other1 AS "Other",
                Notification.IsRepairEngMarkOWCID AS "Eng Mark OW/CID",
                Notification.IsMarkForThirdParty AS "Marked For Third Party",
                Notification.IsMarkForCRMA_MainBranch AS "Marked For CRMA Main Branch",
                Notification.IsSendToCRMA_MainBranch AS "Sent to CRMA Main Branch",
                Notification.CurrentStatus AS "Current Status",
                Notification.FinalClosureDate AS "Final Closure Date",
                Notification.FinalClosureComment AS "Final Closure Comment"
            FROM
                Notification
            JOIN
                BranchMaster
            ON
                Notification.BranchId = BranchMaster.BranchId
            JOIN 
                CustomerMaster
            ON
                Notification.CustomerId = CustomerMaster.CustomerId
            WHERE Notification.NotificationNo IN ({placeholders});
            """

            # Query 2: Issue to engineer history
            query2 = f"""
            SELECT 
                bm.BranchName AS "Branch Name",
                CAST(n.NotificationNo AS VARCHAR) AS "Notification No",
                ie.IssueLevel AS "Issue Level",
                ie.Comment,
                n.IsRepairEngMarkOWCID AS "Eng Mark OW/CID",
                ie.IsPartConsumed AS "Part Consumed",
                ie.IsReleased AS "Released",
                ie.ReleasedDate AS "Realeased Date",
                ie.[Condition] AS "COnditions",
                ie.ReIssueDate AS "Re-Issue Date"
            FROM
                Notification n 
            JOIN
                BranchMaster bm 
            ON
                n.BranchId = bm.BranchId
            JOIN 
                IssueEng ie 
            ON
                n.NotificationId = ie.NotificationId
            WHERE n.NotificationNo IN ({placeholders});
            """

            # Query 3: Notification RTV/Sales History
            query3 = f""" 
            SELECT 
               CAST(n.NotificationNo AS VARCHAR) AS "Notification No",
                r.StockType AS "Type",
                r.ProductCode AS "Product Code",
                r.SerialNo AS "Serial No",
                r.Quantity AS "Quantity",
                r.PortalRefNo AS "Portal Ref No"
            FROM 
                RTVTransfer r               
            JOIN 
                Notification n 
            ON n.NotificationId = r.NotificationId 
            WHERE n.NotificationNo IN ({placeholders});
            """

            # Query 4: Notification Outward History
            query4 = f"""
            SELECT 
               CAST(o.NotificationNo AS VARCHAR) AS "Notification No",
                o.[Action] AS "Action",
                o.ReturnedProductCode AS "Return Product Code",
                o.ReturnedProductSerialNo AS "Returned Product Serial No",
                o.Remark AS "Remark",
                o.AfterRejectionActionRemark AS "Rejection Remark",
                o.CNRef AS "CN Ref",
                o.CNAmount AS "CN Amount",
                o.DNRef AS "DN Ref",
                o.DNAmount AS "DN Amount",
                o.SIRef AS "SI Ref",
                o.SIAmount AS "SI Amount",
                o.VAT_Type AS "VAT Type",
                o.FinalOutwardRemark AS "Outward Remark",
                o.OutwardChallanNo AS "Outward Challan No",
                o.OutwardChallanDate AS "Outward Challan Date",
                o.SAP_CN_DocNo AS "SAP CN Doc No",
                o.SAP_CN_Value AS "SAP CN Value",
                o.SAP_DN_Value AS "SAP DN Value",
                o.SAP_SI_Value AS "SAP SI Value",
                sm.SONumber AS "SO Number"
            FROM Outward o
            JOIN StockMaster sm 
            ON o.NotificationNo = sm.NotificationNo
            WHERE o.NotificationNo IN ({placeholders});
            """

            # Fetch and display results for Query 1
            df1 = pd.read_sql_query(query1, conn, params=notification_nos)
            if not df1.empty:
                st.subheader("Inward Details")
                st.dataframe(df1, use_container_width=True)
            else:
                st.warning("No Inward history found for the entered notification numbers.")

            # Fetch and display results for Query 2
            df2 = pd.read_sql_query(query2, conn, params=notification_nos)
            if not df2.empty:
                st.subheader("Issue to Engineer History")
                st.dataframe(df2, use_container_width=True)
            else:
                st.warning("No issue history found for the entered notification numbers.")

            # Fetch and display results for Query 3
            df3 = pd.read_sql_query(query3, conn, params=notification_nos)
            if not df3.empty:
                st.subheader("Notification RTV/Sales History")
                st.dataframe(df3, use_container_width=True)
            else:
                st.warning("No RTV/Sales history found for the entered notification numbers.")

            # Fetch and display results for Query 4
            df4 = pd.read_sql_query(query4, conn, params=notification_nos)
            if not df4.empty:
                st.subheader("Notification Outward History")
                st.dataframe(df4, use_container_width=True)
            else:
                st.warning("No Outward history found for the entered notification numbers.")

        except Exception as e:
            st.error(f"Query failed: {e}")
        finally:
            if conn:
                conn.close()
