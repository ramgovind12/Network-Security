from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig,DataValidationConfig, DataTransformationConfig, ModelTrainerConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig

import sys

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        dataingestion = DataIngestion(dataingestionconfig)
        logging.info("Initiate the dataingestion")
        dataingestionartifact = dataingestion.initiate_data_ingestion()
        logging.info("data initiation completed")
        print(dataingestionartifact)

        data_validation_config = DataValidationConfig(training_pipeline_config=trainingpipelineconfig)
        data_validation = DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate the data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("Data validation completed.")
        print(data_validation_artifact)

        logging.info("Data Transformation started")
        data_transformation_config = DataTransformationConfig(trainingpipelineconfig)
        data_transformation = DataTransformation(data_validation_artifact,data_transformation_config)
        data_transformation_artifact = data_transformation.initiate_data_transfomation()
        print(data_transformation_artifact)
        logging.info("Data Transformation completed")

        logging.info("Model Trainer started")
        model_trainer_config = ModelTrainerConfig(training_pipeline_config=trainingpipelineconfig)
        model_trainer = ModelTrainer(model_trainer_config,data_transformation_artifact)
        model_trainer_artifact = model_trainer.initiate_model_trainer()
        print(model_trainer_artifact)
        logging.info("Model Trainer completed")
    except Exception as e:
        raise NetworkSecurityException(e,sys)