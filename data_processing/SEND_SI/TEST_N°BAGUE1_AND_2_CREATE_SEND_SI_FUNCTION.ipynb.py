# Databricks notebook source
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "41ecd271-f980-4b40-996f-656dc3699eb0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "import os\n",
    "import re"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b96a2d2e-21c4-44f8-8051-8961b454bc90",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "files = os.listdir('../whole_workflow/Studies_input')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1f720fd0-a347-4876-90eb-045fad79e2d4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "files"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b2e196af-4700-46a8-9a0c-b02f500a48dd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "errors = {}\n",
    "bagues = {}\n",
    "studies_bague = []\n",
    "for file in files:\n",
    "    print(file)\n",
    "    try:\n",
    "        pesee_indiv = pd.read_excel(\n",
    "            os.path.join('../whole_workflow/Studies_input', file),\n",
    "            sheet_name='Pesées individuelles',\n",
    "            header=1\n",
    "            )\n",
    "        bagues_columns = re.findall(r'N° Bague \\d', ' '.join(pesee_indiv.columns.tolist()))\n",
    "        bagues.update({file.split('.')[0]:bagues_columns})\n",
    "        if not np.all(pesee_indiv['N° Bague 1'].isna()) or not np.all(pesee_indiv['N° Bague 2'].isna()):\n",
    "            studies_bague.append(file)\n",
    "            \n",
    "    except Exception as ex:\n",
    "            template = \"An exception of type {0} occurred. Arguments:{1!r}\"\n",
    "            message = template.format(type(ex).__name__, ex.args)\n",
    "            errors.update({file:message})\n",
    "            print('error!')\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e0015bf6-cccd-43ac-b2bc-eb853f0e0c0c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "errors"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "716d581d-e002-4fb1-b71f-4021fb6ebf62",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "bagues"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "cdb2f3e3-6538-4495-b325-2a39c2b0bb6c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "studies_bague"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "11881b5d-4fca-475f-90ca-ead8d4d2280f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "270baf4e-2234-4dbc-b0ba-a20b84a8430b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "pesee_indiv = pd.read_excel('output_send_pesee_indiv_SAIS19-240-TDQv.xlsx')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "535ba86e-90d7-489f-9284-a21ba268314e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "pesee_indiv.info()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3fc03f56-9924-4f0b-be42-66f77ca65313",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "pesee_indiv['N° Bague 2'].unique()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a998d1ba-ecee-4133-abd5-233c4438dd75",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "def create_send_si(pesee_indiv_2:pd.DataFrame)->pd.DataFrame:\n",
    "    \n",
    "    # function creates new subject IDs from labelling (N° Bague 1 and N° Bague 2)\n",
    "    # These new subject IDs will be exchanged with old IDs in SEND_DM\n",
    "    # OSUBJID -> NSUBJID\n",
    "    \n",
    "    final_columns = ['DOMAIN', 'STUDYID', 'EUPOOLID', 'USUBJID', 'OSUBJID', 'NSUBJID']\n",
    "    regex = r'mort|remplacant'\n",
    "\n",
    "\n",
    "    table = (pesee_indiv_2\n",
    "             .rename(columns={'Essai':'STUDYID'})\n",
    "             .assign(\n",
    "                 # filter Mort entries -> np.nan\n",
    "                 NBague1 = lambda df: df['N° Bague 1'].apply(lambda x: np.nan if bool(re.findall(regex, str(x).lower())) else x),\n",
    "                 NBague2 = lambda df: df['N° Bague 2'].apply(lambda x: np.nan if bool(re.findall(regex, str(x).lower())) else x),\n",
    "                 # fill N° Bague 2 with N° Bague 2 (last ID to keep...)\n",
    "                 NBague = lambda df: df['NBague2'].fillna(df['NBague1'], axis=0),\n",
    "\n",
    "             )\n",
    "             .groupby(['STUDYID', 'Parquet', 'NBague']).agg({'régime':'first', 'Traitement':'first', 'Date':'min'})\n",
    "             .reset_index()\n",
    "             .assign(\n",
    "                 NB_fin = lambda df: df['NBague'].apply(lambda x: str(int(x)) if type(x) == float else x),\n",
    "                 USUBJID2 = lambda df: df.groupby(['STUDYID', 'Parquet', 'Date'])['régime'].rank('first').astype(int),\n",
    "                 EUPOOLID = lambda df: df['STUDYID'] + '_CERN_' + df['Parquet'].apply(lambda x: '0' * (3 - len(str(int(x)))) + str(int(x))),\n",
    "                 USUBJID = lambda df: df['EUPOOLID'] + '_' + df['USUBJID2'].apply(lambda x: '0' * (5 - len(str(x))) + str(x) + 'T'),\n",
    "                 NSUBJID = lambda df: df['EUPOOLID'] + '_' + df['NB_fin'].apply(lambda x: (5 - len(str(x))) * '0' + str(x) if str(x).isnumeric() else x),\n",
    "                 OSUBJID = lambda df: df['USUBJID'],\n",
    "                 DOMAIN = 'SI'\n",
    "                 )\n",
    "            )\n",
    "    \n",
    "    return table[final_columns]\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "bee01fd9-2611-40f7-93fc-641385bb48a8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "studies = [file for file in os.listdir() if 'output_send_pesee_indiv' in file]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "cf678109-9b04-453c-8768-d5525b6927ef",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "studies"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8342e6ed-35eb-4ac6-8a83-d5af2627147a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "send_si = []\n",
    "errors = {}\n",
    "for study in studies:\n",
    "    if study == 'output_send_pesee_indiv_SAIS22-156-PRL.xlsx':\n",
    "        print(study.split('_')[-1])\n",
    "        try:\n",
    "            input_table = pd.read_excel(study)\n",
    "            table_output = create_send_si(input_table)\n",
    "            send_si.append(table_output)\n",
    "        except Exception as ex:\n",
    "            template = \"ex type {0},  Arguments:{1!r}\"\n",
    "            message = template.format(type(ex).__name__, ex.args)\n",
    "            errors.update({study:message})\n",
    "            print('error!')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "3fcfb119-9af8-4946-b8b5-c84e45c82237",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "errors"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7d8a07ab-5307-432e-84f6-9f92c6e4c949",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "send_si_studies = pd.concat(send_si)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a818235f-1c16-4a4e-91ec-d5caaf4e51fa",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "send_si_studies.to_excel('SEND_SI_Croissance.xlsx', index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e254ed10-48d8-460c-8d38-6899efaa96d5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0d6c2b36-1e6f-4355-97d1-29832e241344",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8a53b0be-166e-4a7f-b262-97bb1e65db7b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "95aef139-b977-4a5a-a132-800ec0ee1cc4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b30992d2-07f7-4fc2-b668-d464c5cb5266",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7bf8ba76-de8f-4790-8b22-a34266559654",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": null,
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {},
   "notebookName": "TEST_N°BAGUE1_AND_2_CREATE_SEND_SI_FUNCTION",
   "widgets": {}
  },
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}