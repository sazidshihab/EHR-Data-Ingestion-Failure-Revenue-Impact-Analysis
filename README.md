# EHR-Data-Ingestion-Failure-Revenue-Impact-Analysis

Overview  
This project investigates missing Electronic Health Record (EHR) encounters that failed to upload from an EHR system into the main healthcare database. Using SQL analysis, the project identifies patterns causing ingestion failures and estimates their business impact.  

Dataset Summary  
Metric	                 Value  
Total EHR encounters	   6,612  
Records in database    	6,374  
Successfully uploaded	  6,330  
Missing encounters	    282 
Rejection rate	        4.26%  

Key Findings  
Two main patterns caused encounter rejection:  

1. CPT Code Format Issues (78%)  
Many rejected encounters contained non-numeric or improperly formatted CPT codes (e.g., NORCM, sp90, TOS115). When detected, the ingestion system rejected the entire encounter.  
2. Encounter Identifier Mismatch (22%)  
Some encounters had valid CPT codes but failed due to mismatches in patient name, provider, or service date, preventing the system from matching them with database records.    

Business Impact  
Estimated impact if each encounter averages $100 billing value:  
Missing encounters: 282  
Estimated revenue leakage: ~$28,000  
Potential yearly loss (at similar rates): ~$420K  

Proposed Improvements  
Validate CPT formats before ingestion.  
Implement unique patient/provider identifiers instead of name matching.  
Allow flexible date matching to reduce ingestion mismatches.  
Add input validation for CPT codes at the provider level.  

Tools Used  
MySQL  
Retool (dashboard visualization)  
SQL pattern analysis  

Skills Demonstrated  
Data integrity investigation  
SQL-based pattern analysis  
Data pipeline debugging  
Healthcare data analytics  
