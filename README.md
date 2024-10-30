# Fleet-Managament[PDM_FLEET_MV.zip](https://github.com/user-attachments/files/17541323/PDM_FLEET_MV.zip)



[Pricing (1).zip](https://github.com/user-attachments/files/17556890/Pricing.1.zip)



{
    "name": "Flow_FLEET_T",
    "properties": {
        "activities": [
            {
                "name": "PDM_Fleet_monthly_refresh",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "notebookPath": "/Pricing/PDM_FLEET_MV/PDM_Fleet_monthly_refresh",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "tabulate"
                            }
                        },
                        {
                            "pypi": {
                                "package": "holidays"
                            }
                        },
                        {
                            "pypi": {
                                "package": "mailjet_rest"
                            }
                        }
                    ]
                },
                "linkedServiceName": {
                    "referenceName": "Databricks_new_cluster_az",
                    "type": "LinkedServiceReference",
                    "parameters": {
                        "url": {
                            "value": "@pipeline().globalParameters.DATABRICKS_WORKSPACE_URL",
                            "type": "Expression"
                        },
                        "secret_name": {
                            "value": "@pipeline().globalParameters.KV_SECRET_DATABRICKS_TOKEN",
                            "type": "Expression"
                        }
                    }
                }
            },
            {
                "name": "Set var run SQL",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "PDM_Fleet_monthly_refresh",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "variableName": "run_sql_out",
                    "value": {
                        "value": "@string(activity('PDM_Fleet_monthly_refresh').output.runOutput.run_sql_out)",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "If run SQL Fleet",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "Set var run SQL",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "expression": {
                        "value": "@or(equals(variables('run_sql_out'),'1'), equals(variables('run_sql_out'),'3'))",
                        "type": "Expression"
                    },
                    "ifFalseActivities": [
                        {
                            "name": "Wait_False",
                            "type": "Wait",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "waitTimeInSeconds": 1
                            }
                        }
                    ],
                    "ifTrueActivities": [
                        {
                            "name": "Execute PDM_FLEET_MV_copy1",
                            "type": "ExecutePipeline",
                            "dependsOn": [
                                {
                                    "activity": "PDM_FLEET_T",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "policy": {
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "PDM_FLEET_MV",
                                    "type": "PipelineReference"
                                },
                                "waitOnCompletion": true
                            }
                        },
                        {
                            "name": "PDM_FLEET_T",
                            "type": "ExecutePipeline",
                            "dependsOn": [],
                            "policy": {
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "PDM_FLEET_T",
                                    "type": "PipelineReference"
                                },
                                "waitOnCompletion": true
                            }
                        }
                    ]
                }
            },
            {
                "name": "If run SQL copy prev MTH and DS_copy1",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "Set var run SQL",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "expression": {
                        "value": "@equals(variables('run_sql_out'),'3')",
                        "type": "Expression"
                    },
                    "ifFalseActivities": [
                        {
                            "name": "Wait_False_copy1",
                            "type": "Wait",
                            "dependsOn": [],
                            "userProperties": [],
                            "typeProperties": {
                                "waitTimeInSeconds": 1
                            }
                        }
                    ],
                    "ifTrueActivities": [
                        {
                            "name": "PDM_FLEET_T_PREV_MTH",
                            "type": "ExecutePipeline",
                            "dependsOn": [],
                            "policy": {
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "AFS_Transfer",
                                    "type": "PipelineReference"
                                },
                                "waitOnCompletion": true
                            }
                        },
                        {
                            "name": "PDM_FLEET_T_2_MTH_OLD",
                            "type": "ExecutePipeline",
                            "dependsOn": [
                                {
                                    "activity": "PDM_FLEET_T_PREV_MTH",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "policy": {
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "AFS_Transfer",
                                    "type": "PipelineReference"
                                },
                                "waitOnCompletion": true
                            }
                        },
                        {
                            "name": "Ex Fleet to DS",
                            "type": "ExecutePipeline",
                            "dependsOn": [
                                {
                                    "activity": "PDM_FLEET_T_PREV_MTH",
                                    "dependencyConditions": [
                                        "Succeeded"
                                    ]
                                }
                            ],
                            "policy": {
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "Fleet_DataCap_to_DS",
                                    "type": "PipelineReference"
                                },
                                "waitOnCompletion": true
                            }
                        }
                    ]
                }
            },
            {
                "name": "Send Mail Error 3",
                "type": "ExecutePipeline",
                "dependsOn": [
                    {
                        "activity": "If run SQL Fleet",
                        "dependencyConditions": [
                            "Failed"
                        ]
                    }
                ],
                "policy": {
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "pipeline": {
                        "referenceName": "Mail_JSON",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": true,
                    "parameters": {
                        "EmailTo": "\"jaimenoe.benito@aldautomotive.com\"",
                        "HTMLMessage": "[PDM] ERROR on PDM_FLEET_T table copied to the SQL",
                        "Pipeline": {
                            "value": "@pipeline().Pipeline",
                            "type": "Expression"
                        },
                        "Subject": "[DEV] [PDM] ERROR on PDM_FLEET_T table copied to the SQL",
                        "Status": "Error"
                    }
                }
            },
            {
                "name": "Send Mail Error 2",
                "type": "ExecutePipeline",
                "dependsOn": [
                    {
                        "activity": "If run SQL copy prev MTH and DS_copy1",
                        "dependencyConditions": [
                            "Failed"
                        ]
                    }
                ],
                "policy": {
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "pipeline": {
                        "referenceName": "Mail_JSON",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": true,
                    "parameters": {
                        "EmailTo": "\"jaimenoe.benito@aldautomotive.com\"",
                        "HTMLMessage": "[PDM] ERROR on PDM_FLEET PRV MONTH table copied to the SQL",
                        "Pipeline": {
                            "value": "@pipeline().Pipeline",
                            "type": "Expression"
                        },
                        "Subject": "[DEV] [PDM] ERROR on PDM_FLEET PRV MONTH  table copied to the SQL",
                        "Status": "Error"
                    }
                }
            },
            {
                "name": "Send Mail Error",
                "type": "ExecutePipeline",
                "dependsOn": [
                    {
                        "activity": "PDM_Fleet_monthly_refresh",
                        "dependencyConditions": [
                            "Failed"
                        ]
                    }
                ],
                "policy": {
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "pipeline": {
                        "referenceName": "Mail_JSON",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": true,
                    "parameters": {
                        "EmailTo": "\"jaimenoe.benito@aldautomotive.com\"",
                        "HTMLMessage": "[PDM] ERROR on FLEET databricks",
                        "Pipeline": {
                            "value": "@pipeline().Pipeline",
                            "type": "Expression"
                        },
                        "Subject": "[DEV] [PDM]  [PDM] ERROR on FLEET databricks",
                        "Status": "Error"
                    }
                }
            }
        ],
        "variables": {
            "run_sql_out": {
                "type": "String",
                "defaultValue": "0"
            }
        },
        "folder": {
            "name": "PRICING/FLEET"
        },
        "annotations": [],
        "lastPublishTime": "2024-05-17T08:55:37Z"
    },
    "type": "Microsoft.DataFactory/factories/pipelines"
}
