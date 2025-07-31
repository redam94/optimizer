# grpc_server/budget_optimizer_server.py

import grpc
import logging
import time
from concurrent import futures
from typing import Dict
from datetime import datetime
import multiprocessing as mp
import optuna
from pathlib import Path
import traceback

# Generated from proto files
from grpc_service.generated import budget_optimizer_pb2_grpc
from grpc_service.generated import budget_optimizer_pb2

# Existing budget optimizer imports
from backend.utils.budget_classes import BudgetScenario as BudgetScenarioModel, Budget as BudgetModel
from backend.model_settings.optimizer import revenue_model, create_optimizer
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf import struct_pb2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptimizationProcess(mp.Process):
    """Background process for running optimization"""
    
    def __init__(self, database_url: str, budget_scenario: dict, timeout: int, n_trials: int):
        super().__init__()
        self.daemon = True
        self.database_url = database_url
        self.budget_scenario = budget_scenario
        self.timeout = timeout
        self.n_trials = n_trials
        self._progress_queue = mp.Queue()
        self._stop_event = mp.Event()
    
    def run(self):
        """Run the optimization process"""
        try:
            logger.info(f"Starting optimization for {self.budget_scenario['name']}")
            
            # Convert grpc message to internal format
            scenario = self._convert_scenario(self.budget_scenario)
            
            # Create optimizer
            config_path = Path(__file__).parent / "backend/model_settings/example_files"
            optimizer = create_optimizer(self.database_url, config_path)
            
            # Setup bounds and constraints
            bounds = self._get_bounds(scenario)
            constraints = self._get_constraints(scenario)
            
            logger.info(f"Bounds: {bounds}, Constraints: {constraints}")
            
            # Run optimization
            optimizer.optimize(
                bounds,
                constraints=constraints,
                study_name=scenario.name,
                n_trials=self.n_trials,
                n_jobs=1,
                timeout=self.timeout * 60,  # Convert to seconds
                load_if_exists=False
            )
            
            self._progress_queue.put({
                'status': 'completed',
                'message': f'Optimization completed for {scenario.name}'
            })
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            self._progress_queue.put({
                'status': 'failed',
                'error': str(e),
                'traceback': traceback.format_exc()
            })
    
    def _convert_scenario(self, scenario_dict: dict) -> BudgetScenarioModel:
        """Convert gRPC scenario to internal model"""
        # This would need to be implemented based on your internal model structure
        # For now, returning a mock object
        class MockScenario:
            def __init__(self, data):
                self.name = data['name']
                self.timeout = data.get('timeout', 60)
                self.n_trials = data.get('n_trials', 100)
        
        return MockScenario(scenario_dict)
    
    def _get_bounds(self, scenario) -> dict:
        """Extract bounds from scenario"""
        # Mock implementation - replace with actual bounds extraction
        return {
            'OLV': (5, 15),
            'Paid Search': (5, 15),
            'Print': (5, 15),
            'Radio': (5, 15)
        }
    
    def _get_constraints(self, scenario) -> tuple:
        """Extract constraints from scenario"""
        # Mock implementation - replace with actual constraint extraction
        return (40, 45)  # Total budget constraints
    
    def get_progress(self):
        """Get progress updates"""
        updates = []
        while not self._progress_queue.empty():
            updates.append(self._progress_queue.get())
        return updates


class BudgetOptimizerServicer(budget_optimizer_pb2_grpc.BudgetOptimizerServiceServicer):
    """gRPC service implementation for Budget Optimizer"""
    
    def __init__(self, database_url: str = "sqlite:///budget_optimizer.db"):
        self.database_url = database_url
        self.running_processes: Dict[str, OptimizationProcess] = {}
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database connection"""
        # Setup database similar to FastAPI startup
        pass
    
    def CreateBudgetScenario(self, request, context):
        """Create a new budget scenario and start optimization"""
        try:
            scenario = request.scenario
            scenario_name = scenario.name
            
            logger.info(f"Creating budget scenario: {scenario_name}")
            
            # Check if scenario already exists
            if scenario_name in optuna.study.get_all_study_names(storage=self.database_url):
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Budget scenario '{scenario_name}' already exists")
                return budget_optimizer_pb2.CreateBudgetScenarioResponse(
                    success=False,
                    error=f"Budget scenario '{scenario_name}' already exists"
                )
            
            # Check if already running
            if scenario_name in self.running_processes:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Budget scenario '{scenario_name}' is already running")
                return budget_optimizer_pb2.CreateBudgetScenarioResponse(
                    success=False,
                    error=f"Budget scenario '{scenario_name}' is already running"
                )
            
            # Convert scenario to dict for process
            scenario_dict = {
                'name': scenario.name,
                'timeout': scenario.timeout,
                'n_trials': scenario.n_trials,
                'olv': {
                    'initial_budget': scenario.olv.initial_budget,
                    'lower_bound': scenario.olv.lower_bound,
                    'upper_bound': scenario.olv.upper_bound
                },
                'paid_search': {
                    'initial_budget': scenario.paid_search.initial_budget,
                    'lower_bound': scenario.paid_search.lower_bound,
                    'upper_bound': scenario.paid_search.upper_bound
                },
                'print': {
                    'initial_budget': scenario.print.initial_budget,
                    'lower_bound': scenario.print.lower_bound,
                    'upper_bound': scenario.print.upper_bound
                },
                'radio': {
                    'initial_budget': scenario.radio.initial_budget,
                    'lower_bound': scenario.radio.lower_bound,
                    'upper_bound': scenario.radio.upper_bound
                },
                'total_budget': {
                    'initial_budget': scenario.total_budget.initial_budget,
                    'lower_bound': scenario.total_budget.lower_bound,
                    'upper_bound': scenario.total_budget.upper_bound
                }
            }
            
            # Start optimization process
            process = OptimizationProcess(
                self.database_url,
                scenario_dict,
                scenario.timeout,
                scenario.n_trials
            )
            process.start()
            self.running_processes[scenario_name] = process
            
            return budget_optimizer_pb2.CreateBudgetScenarioResponse(
                success=True,
                message=f"Optimization started for {scenario_name}",
                scenario_name=scenario_name
            )
        
        except Exception as e:
            logger.error(f"Error creating budget scenario: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return budget_optimizer_pb2.CreateBudgetScenarioResponse(
                success=False,
                error=str(e)
            )
    
    def ListBudgetScenarios(self, request, context):
        """List all budget scenarios"""
        try:
            scenario_names = optuna.study.get_all_study_names(storage=self.database_url)
            return budget_optimizer_pb2.ListBudgetScenariosResponse(
                scenario_names=scenario_names
            )
        except Exception as e:
            logger.error(f"Error listing scenarios: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return budget_optimizer_pb2.ListBudgetScenariosResponse(scenario_names=[])
    
    def GetBudgetScenario(self, request, context):
        """Get details of a specific budget scenario"""
        try:
            scenario_name = request.name
            
            # Load study from Optuna
            study = optuna.study.load_study(
                study_name=scenario_name,
                storage=self.database_url
            )
            
            # Convert trials to gRPC format
            trials = []
            for trial in study.trials:
                grpc_trial = budget_optimizer_pb2.Trial(
                    trial_id=trial.number,
                    budget=trial.user_attrs.get('budget', {}),
                    values=trial.values if trial.values else [],
                    completed=(trial.state == optuna.trial.TrialState.COMPLETE)
                )
                
                # Set timestamp
                if trial.datetime_start:
                    timestamp = Timestamp()
                    timestamp.FromDatetime(trial.datetime_start)
                    grpc_trial.created_at.CopyFrom(timestamp)
                
                trials.append(grpc_trial)
            
            # Create study response
            grpc_study = budget_optimizer_pb2.Study(
                name=scenario_name,
                trials=trials,
                status="completed" if study.trials else "running"
            )
            
            # Set timestamps
            if study.trials:
                first_trial = min(study.trials, key=lambda t: t.datetime_start or datetime.min)
                last_trial = max(study.trials, key=lambda t: t.datetime_start or datetime.min)
                
                if first_trial.datetime_start:
                    created_timestamp = Timestamp()
                    created_timestamp.FromDatetime(first_trial.datetime_start)
                    grpc_study.created_at.CopyFrom(created_timestamp)
                
                if last_trial.datetime_start:
                    updated_timestamp = Timestamp()
                    updated_timestamp.FromDatetime(last_trial.datetime_start)
                    grpc_study.last_updated.CopyFrom(updated_timestamp)
            
            return budget_optimizer_pb2.GetBudgetScenarioResponse(
                study=grpc_study,
                found=True
            )
        
        except Exception as e:
            logger.error(f"Error getting scenario {request.name}: {e}")
            return budget_optimizer_pb2.GetBudgetScenarioResponse(
                found=False,
                error=str(e)
            )
    
    def GetBestTrial(self, request, context):
        """Get the best trial for a scenario"""
        try:
            scenario_name = request.name
            study = optuna.study.load_study(study_name=scenario_name, storage=self.database_url)
            
            if not study.trials:
                return budget_optimizer_pb2.GetBestTrialResponse(
                    found=False,
                    error="No trials found for this scenario"
                )
            
            best_trial = study.best_trial
            
            # Convert to gRPC format
            grpc_trial = budget_optimizer_pb2.Trial(
                trial_id=best_trial.number,
                budget=best_trial.user_attrs.get('budget', {}),
                values=best_trial.values if best_trial.values else [],
                completed=True
            )
            
            best_trial_response = budget_optimizer_pb2.BestTrial(
                trial=grpc_trial,
                objective_value=best_trial.value if best_trial.value else 0.0,
                total_trials=len(study.trials)
            )
            
            return budget_optimizer_pb2.GetBestTrialResponse(
                best_trial=best_trial_response,
                found=True
            )
        
        except Exception as e:
            logger.error(f"Error getting best trial for {request.name}: {e}")
            return budget_optimizer_pb2.GetBestTrialResponse(
                found=False,
                error=str(e)
            )
    
    def DeleteBudgetScenario(self, request, context):
        """Delete a budget scenario"""
        try:
            scenario_name = request.name
            
            # Stop running process if exists
            if scenario_name in self.running_processes:
                process = self.running_processes[scenario_name]
                process.terminate()
                process.join(timeout=5)
                del self.running_processes[scenario_name]
            
            # Delete from Optuna
            optuna.study.delete_study(study_name=scenario_name, storage=self.database_url)
            
            return budget_optimizer_pb2.DeleteBudgetScenarioResponse(
                success=True,
                message=f"Deleted scenario: {scenario_name}"
            )
        
        except Exception as e:
            logger.error(f"Error deleting scenario {request.name}: {e}")
            return budget_optimizer_pb2.DeleteBudgetScenarioResponse(
                success=False,
                error=str(e)
            )
    
    def Predict(self, request, context):
        """Make a revenue prediction"""
        try:
            budget = request.budget
            
            # Convert to internal budget format
            budget_dict = {
                'OLV': budget.olv,
                'Paid Search': budget.paid_search,
                'Print': budget.print,
                'Radio': budget.radio
            }
            
            # Make prediction using the revenue model
            prediction = revenue_model.predict(budget_dict).sum(...).item()
            
            # Calculate contributions (simplified)
            total_spend = sum(budget_dict.values())
            contributions = {}
            if total_spend > 0:
                for channel, spend in budget_dict.items():
                    contributions[channel] = spend / total_spend * prediction
            
            return budget_optimizer_pb2.PredictResponse(
                prediction=prediction,
                currency="USD",
                channel_contributions=contributions
            )
        
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return budget_optimizer_pb2.PredictResponse(prediction=0.0)
    
    def StreamOptimization(self, request, context):
        """Stream optimization progress updates"""
        scenario_name = request.scenario_name
        poll_interval = max(request.poll_interval_seconds, 1)  # At least 1 second
        
        logger.info(f"Starting optimization stream for {scenario_name}")
        
        try:
            while True:
                # Check if process is running
                if scenario_name in self.running_processes:
                    process = self.running_processes[scenario_name]
                    
                    # Check if process is still alive
                    if not process.is_alive():
                        # Process finished, send final update
                        yield budget_optimizer_pb2.OptimizationUpdate(
                            scenario_name=scenario_name,
                            status="completed",
                            timestamp=self._get_current_timestamp()
                        )
                        break
                    
                    # Get progress updates
                    updates = process.get_progress()
                    for update in updates:
                        yield budget_optimizer_pb2.OptimizationUpdate(
                            scenario_name=scenario_name,
                            status=update.get('status', 'running'),
                            timestamp=self._get_current_timestamp()
                        )
                
                # Try to get current best trial from database
                try:
                    study = optuna.study.load_study(study_name=scenario_name, storage=self.database_url)
                    if study.trials:
                        best_trial = study.best_trial
                        yield budget_optimizer_pb2.OptimizationUpdate(
                            scenario_name=scenario_name,
                            current_trial=len(study.trials),
                            best_value=best_trial.value if best_trial.value else 0.0,
                            best_budget=best_trial.user_attrs.get('budget', {}),
                            status="running",
                            timestamp=self._get_current_timestamp()
                        )
                except:
                    # Study not found yet, continue
                    pass
                
                # Wait before next update
                time.sleep(poll_interval)
        
        except Exception as e:
            logger.error(f"Error in optimization stream: {e}")
            yield budget_optimizer_pb2.OptimizationUpdate(
                scenario_name=scenario_name,
                status="error",
                timestamp=self._get_current_timestamp()
            )
    
    def HealthCheck(self, request, context):
        """Health check endpoint"""
        try:
            # Perform basic health checks
            service = request.service or "BudgetOptimizerService"
            
            # Check database connection
            scenario_names = optuna.study.get_all_study_names(storage=self.database_url)
            
            return budget_optimizer_pb2.HealthCheckResponse(
                status=budget_optimizer_pb2.HealthCheckResponse.SERVING,
                message=f"{service} is healthy. Found {len(scenario_names)} scenarios.",
                timestamp=self._get_current_timestamp()
            )
        
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return budget_optimizer_pb2.HealthCheckResponse(
                status=budget_optimizer_pb2.HealthCheckResponse.NOT_SERVING,
                message=f"Service unhealthy: {str(e)}",
                timestamp=self._get_current_timestamp()
            )
    
    def _get_current_timestamp(self) -> Timestamp:
        """Get current timestamp in protobuf format"""
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        return timestamp


def serve():
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add servicers
    budget_optimizer_pb2_grpc.add_BudgetOptimizerServiceServicer_to_server(
        BudgetOptimizerServicer(),
        server
    )
    
    # Configure server
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    # Start server
    server.start()
    logger.info(f"Budget Optimizer gRPC server started on {listen_addr}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down gRPC server...")
        server.stop(5)  # 5 second grace period


if __name__ == '__main__':
    serve()