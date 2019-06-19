
## Package Overview

This package was created to streamline the process of updating and prioritizing 
trafficking case records for use by the investigations team. It pulls the latest
Case Information Forms from the Searchlight database and adds them to the 
Case Dispatcher Google Sheets. The primary sheets here are the 'Entity Groups'
of 'Suspects', 'Police', and 'Victims', each of which gets updated with the latest
case follow up information from investigations in Google Sheets. The scripts in
this package preserve those updates while adding new cases and moving cases which
have been marked as closed in any of the Entity Groups sheets to corresponding 
'Closed' sheets which preserve outcome data. 'Suspects' and 'Police' sheets are
suspect-centric, with each observation consisting of data on one suspect, while
the 'Victims' sheets store data on one victim per observation. The sheets are linked
by a common case ID, and when all of the suspect records are marked as closed
for a given case either in the 'Suspects' or 'Police' sheet, all of the victim
records for that case are also moved to the 'Closed' sheet, and vice versa. Cases are
closed when each Entity Group step has been successfully completed and the suspect
is arrested or when all leads have been exhausted.

A classifier is used (default is Random Forest) to calculate a Strength of Case (SoC) 
score for each suspect record which is intended to estimate the likelihood that the 
case will result in an arrest. Grid Search Cross Validation can optionally be run
to obtain the current optimum classifier parameters. A 'Solvability' score is also
calculated according to predefined conditions and step completions for that
case/individual. Finally, an 'Eminence' score, which estimates the subjective importance
of a case, is pulled from the Case Dispatcher Google Sheet where it was manually 
entered by the Director of Investigations. A weighted average of these three scores
is calculated using the weights assigned in the 'Parameters' Google Sheet, and this
produces a priority score between 0 and 1 which is used to sort the records in each of 
the Entity Groups sheets so that the highest priority cases are listed at the top.


## How to Install

The setup.py script in this package provides requirements and other information 
needed for installation. 


## Environment Variables

This package was created using the Anaconda distribution and a virtual environment
running the 32 bit version of Python.
