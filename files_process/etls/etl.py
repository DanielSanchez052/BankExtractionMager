import os
import json
import time
from importlib.machinery import SourceFileLoader

import pandas as pd

from files_process.etls.pipeline import Step, Pipeline, Transform, Load
from settings import Settings
import logger


class ETL:
    def __init__(self, settings: Settings):
        """
        Inicializa el proceso ETL

        Args:
            settings: Configuración del proceso ETL
        """
        self.settings = settings
        self.start_time = time.time()
        self.tasks_data = self._load_tasks()
        self.logger = logger.setup_logger("etl")

    def _load_tasks(self) -> dict:
        """
        Carga las tareas desde el archivo de configuración

        Returns:
            dict: Datos de las tareas
        """
        with open(self.settings.tasks_file) as tasks_file:
            return json.load(tasks_file)

    def _create_extract_provider(self, task: dict, *args, **kwargs) -> Step:
        """
        Crea el proveedor de extracción para una tarea

        Args:
            task: Datos de la tarea

        Returns:
            Step: Proveedor de extracción configurado
        """
        extract_provider_module = SourceFileLoader(
            "extract_provider",
            task["extract_provider"]["script"]
        ).load_module()

        kwargs.update(task["extract_provider"]["args"])
        kwargs.update(self.settings.__dict__)
        kwargs.update({"logger": self.logger})

        return Step(
            getattr(extract_provider_module, task["extract_provider"]["method"]),
            *args,
            **kwargs
        )

    def _create_step_provider(self, step: dict, *args, **kwargs) -> Step:
        """
        Crea un proveedor de paso según su tipo

        Args:
            step: Datos del paso

        Returns:
            Step/Transform/Load: Proveedor de paso configurado
        """
        step_provider_module = SourceFileLoader(step["name"], step["script"]).load_module()
        step_provider = getattr(step_provider_module, step["method"])

        kwargs.update(step["args"])
        kwargs.update(self.settings.__dict__)
        kwargs.update({"logger": self.logger})

        if step["type"] == "Transform":
            return Transform(step_provider, *args, **kwargs)
        elif step["type"] == "Step":
            return Step(step_provider, *args, **kwargs)
        elif step["type"] == "Load":
            return Load(step_provider, *args, **kwargs)

    def _create_load_provider(self, task: dict, *args, **kwargs) -> Load:
        """
        Crea el proveedor de carga para una tarea

        Args:
            task: Datos de la tarea

        Returns:
            Load: Proveedor de carga configurado
        """
        load_provider_module = SourceFileLoader(
            "load_provider",
            task["load_provider"]["script"]
        ).load_module()

        kwargs.update(task["load_provider"]["args"])
        kwargs.update(self.settings.__dict__)
        kwargs.update({"logger": self.logger})

        return Load(
            getattr(load_provider_module, task["load_provider"]["method"]),
            *args,
            **kwargs
        )

    def _create_post_load_provider(self, task: dict, *args, **kwargs) -> Step:
        """
        Crea el proveedor de post-carga para una tarea

        Args:
            task: Datos de la tarea

        Returns:
            Step: Proveedor de post-carga configurado
        """
        if "post_load_provider" in task:
            post_load_provider_module = SourceFileLoader(
                "post_load_provider",
                task["post_load_provider"]["script"]
            ).load_module()

            kwargs.update(task["post_load_provider"]["args"])
            kwargs.update(self.settings.__dict__)
            kwargs.update({"logger": self.logger})

            return Step(
                getattr(post_load_provider_module, task["post_load_provider"]["method"]),
                *args,
                **kwargs
            )
        return None

    def _process_task(self, task: dict, *args, **kwargs) -> pd.DataFrame:
        """
        Procesa una tarea individual

        Args:
            task: Datos de la tarea a procesar
        """
        self.logger.info(f"Executing {task['name']} task")

        # Crear proveedores
        extract_provider = self._create_extract_provider(task, *args, **kwargs)
        steps = [self._create_step_provider(step, *args, **kwargs) for step in task["steps"]]
        load_provider = self._create_load_provider(task, *args, **kwargs)
        post_load_provider = self._create_post_load_provider(task, *args, **kwargs)
        # Crear y ejecutar pipeline
        pipeline = Pipeline(
            source=None,
            extract=extract_provider,
            steps=steps,
            load=load_provider,
            post_load=post_load_provider,
            logger=self.logger,
        )
        return pipeline.run(*args, **kwargs)

    def run(self, task_name: str, **kwargs) -> pd.DataFrame:
        if task_name:
            task = next((t for t in self.tasks_data if t["key"] == task_name), None)
            if task:
                return self._process_task(task, **kwargs)
            else:
                self.logger.error(f"No se encontró la tarea con nombre: {task_name}")
        return pd.DataFrame()
