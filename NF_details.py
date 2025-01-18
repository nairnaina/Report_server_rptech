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

            # Prepare query condition
            placeholders = ",".join("?" for _ in notification_nos)
#queries
            # Query 1: Inward details
            query1 = f"""
            SELECT 
                BranchMaster.BranchName AS "Branch Name",
                CAST(Notification.NotificationNo AS VARCHAR) AS "Notification No",
                Notification.CustomerCode AS "Customer Code",
                CustomerMaster.CustomerName AS "Customer Name",
                CustomerMaster.PhoneNo AS "Phone No.",
                CustomerMaster.MobileNo AS "Mobile No.",
                CustomerMaster.EmailId AS "Email Id",
                CustomerMaster.City AS "City",
                Notification.CustomerChallanNo AS "Customer Challan No.",
                Notification.CustomerInvoiceNo AS "Customer Invoice No.",
                Notification.ChallanNo AS "Challan No.",
                Notification.CreatedOn AS "Inward Date",
                Notification.ProductCode AS "Product Code",
                Notification.ProductSerialNo AS "Product Serial NO.",
                Notification.IsInWarranty AS "Warranty Status",
                Notification.WarrantyDate AS "Warranty Date",
                Notification.Remarks,
                Notification.Other1 AS "Other",
                Notification.IsRepairEngMarkOWCID AS "Eng Mark OW/CID",
                Notification.IsMoveToOwnStock AS "MTOS",
                Notification.MoveToOwnStockDate AS "MTOS Date",
                Notification.IsMarkForThirdParty AS "Marked For Third Party",
                Notification.IsMarkForCRMA_MainBranch AS "Marked For CRMA Main Branch",
                Notification.IsSendToCRMA_MainBranch AS "Sent to CRMA Main Branch",
                ie.[Condition] AS "COnditions",
                Notification.CurrentStatus AS "Current Status",
                Notification.IsVirtualClosure AS "TAT Closed",
                Notification.VirtualClosureDate AS "TAT Closed Date",
                Notification.VirtualClosureComment AS "TAT Closed Comment",
                Notification.IsFinalClosure AS "Customer Closed",
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
            JOIN 
                IssueEng ie 
            ON
                Notification.NotificationId = ie.NotificationId
             WHERE Notification.NotificationNo IN ({placeholders});
            """

            # Query 2: Issue to engineer history
            query2 = f"""
            SELECT 
                bm.BranchName AS "Branch Name",
                CAST(n.NotificationNo AS VARCHAR) AS "Notification No",
                n.Quantity AS " Inward Quantity",
                ie.IssueEngId AS "Portal Eng Id",
                u.UserId AS "User Name",
                ie.CreatedOn AS "Issued on",
                ie.IssueLevel AS "Issue level",
                ie.Comment,
                ie.IsAcknowledged AS "Eng Attempted",
                n.IsRepairEngMarkOWCID AS "Eng Mark OW/CID",
                ie.MarkOWCIDDate AS "OWCID Date",
                ie.IsPartConsumed AS "Part Consumed",
                ie.IsReleased AS "Released",
                ie.ReleasedDate AS "Released Date",
                ie.[Condition] AS "Condition",
                ie.ReIssueDate AS "Re-issue Date"--no data in table
            FROM
                RashiPortal_Live.dbo.Notification n 
            JOIN
                BranchMaster bm 
            ON
                n.BranchId = bm.BranchId
            JOIN 
                IssueEng ie 
            ON
                n.NotificationId = ie.NotificationId
            JOIN 
                usermaster u 
            ON 	
                u.UserId = ie.EngineerId 
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
            --SAP Doc No. (no data in the table)
                r.PortalRefNo AS "Portal Ref No" --no data in table
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
                o.Quantity AS "Returned Quantity",
                o.ReturnedProductSerialNo AS "Returned Product Serial No",
                o.Remark AS "Remark",
                o.IsMarkDeliverForVirtualClosure AS "Marked Delivery",
                n.VirtualClosureDate AS "TAT Closed Date",
                o.AfterRejectionActionRemark AS "Rejection Remark",
                o.CNRef AS "CN Ref",
                o.CNAmount AS "CN Amount",
                o.DNRef AS "DN Ref", --no data in table
                o.DNAmount AS "DN Amount",--no data in table
                o.SIRef AS "SI Ref",--no data in table
                o.SIAmount AS "SI Amount",--no data in table
                o.VAT_Type AS "VAT Type",--no data in table
                o.FinalOutwardRemark AS "Outward Remark",--no data in table
                o.OutwardChallanNo AS "Outward Challan No",--no data in table
                o.OutwardChallanDate AS "Outward Challan Date",--no data in table
                o.SAP_CN_DocNo AS "SAP CN Doc No",
                o.SAP_CN_Value AS "SAP CN Value",
                o.SAP_DN_Value AS "SAP DN Value",--no data in table
                o.SAP_SI_Value AS "SAP SI Value",--no data in table
                sm.SONumber AS "SO Number"--no data in table
            FROM Outward o
            JOIN StockMaster sm 
            ON o.NotificationNo = sm.NotificationNo
            JOIN 
                Notification n 
            ON o.NotificationId = n.NotificationId 
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
