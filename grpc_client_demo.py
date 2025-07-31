# grpc_client/budget_optimizer_client.py

import grpc
import asyncio
import logging
from typing import Optional, Iterator, Dict, Any
import time
from datetime import datetime

# Generated from proto files
from grpc_service.generated import budget_optimizer_pb2
from grpc_service.generated import budget_optimizer_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BudgetOptimizerClient:
    """Client for Budget Optimizer gRPC service"""
    
    def __init__(self, host: str = 'localhost', port: int = 50051):
        self.address = f'{host}:{port}'
        self.channel = None
        self.stub = None
    
    def connect(self):
        """Establish connection to gRPC server"""
        self.channel = grpc.insecure_channel(self.address)
        self.stub = budget_optimizer_pb2_grpc.BudgetOptimizerServiceStub(self.channel)
        logger.info(f"Connected to Budget Optimizer gRPC server at {self.address}")
    
    def disconnect(self):
        """Close connection to gRPC server"""
        if self.channel:
            self.channel.close()
            logger.info("Disconnected from gRPC server")
    
    def health_check(self, service: str = "BudgetOptimizerService") -> bool:
        """Check if the service is healthy"""
        try:
            request = budget_optimizer_pb2.HealthCheckRequest(service=service)
            response = self.stub.HealthCheck(request)
            
            is_healthy = response.status == budget_optimizer_pb2.HealthCheckResponse.SERVING
            logger.info(f"Health check: {response.message}")
            return is_healthy
        
        except grpc.RpcError as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def create_budget_scenario(self, 
                             name: str,
                             channel_budgets: Dict[str, Dict[str, float]],
                             total_budget: Dict[str, float],
                             timeout: int = 60,
                             n_trials: int = 100) -> bool:
        """Create a new budget scenario"""
        try:
            # Create channel budget objects
            olv_budget = budget_optimizer_pb2.ChannelBudget(
                unit=budget_optimizer_pb2.Unit.THOUSAND,
                initial_budget=channel_budgets['olv']['initial'],
                lower_bound=channel_budgets['olv']['lower'],
                upper_bound=channel_budgets['olv']['upper']
            )
            
            paid_search_budget = budget_optimizer_pb2.ChannelBudget(
                unit=budget_optimizer_pb2.Unit.THOUSAND,
                initial_budget=channel_budgets['paid_search']['initial'],
                lower_bound=channel_budgets['paid_search']['lower'],
                upper_bound=channel_budgets['paid_search']['upper']
            )
            
            print_budget = budget_optimizer_pb2.ChannelBudget(
                unit=budget_optimizer_pb2.Unit.THOUSAND,
                initial_budget=channel_budgets['print']['initial'],
                lower_bound=channel_budgets['print']['lower'],
                upper_bound=channel_budgets['print']['upper']
            )
            
            radio_budget = budget_optimizer_pb2.ChannelBudget(
                unit=budget_optimizer_pb2.Unit.THOUSAND,
                initial_budget=channel_budgets['radio']['initial'],
                lower_bound=channel_budgets['radio']['lower'],
                upper_bound=channel_budgets['radio']['upper']
            )
            
            total_budget_obj = budget_optimizer_pb2.ChannelBudget(
                unit=budget_optimizer_pb2.Unit.THOUSAND,
                initial_budget=total_budget['initial'],
                lower_bound=total_budget['lower'],
                upper_bound=total_budget['upper']
            )
            
            # Create scenario
            scenario = budget_optimizer_pb2.BudgetScenario(
                name=name,
                olv=olv_budget,
                paid_search=paid_search_budget,
                print=print_budget,
                radio=radio_budget,
                total_budget=total_budget_obj,
                timeout=timeout,
                n_trials=n_trials
            )
            
            # Make request
            request = budget_optimizer_pb2.CreateBudgetScenarioRequest(scenario=scenario)
            response = self.stub.CreateBudgetScenario(request)
            
            if response.success:
                logger.info(f"Created scenario: {response.message}")
                return True
            else:
                logger.error(f"Failed to create scenario: {response.error}")
                return False
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error creating scenario: {e}")
            return False
    
    def list_scenarios(self) -> list[str]:
        """List all budget scenarios"""
        try:
            request = budget_optimizer_pb2.ListBudgetScenariosRequest()
            response = self.stub.ListBudgetScenarios(request)
            return list(response.scenario_names)
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error listing scenarios: {e}")
            return []
    
    def get_scenario(self, name: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific scenario"""
        try:
            request = budget_optimizer_pb2.GetBudgetScenarioRequest(name=name)
            response = self.stub.GetBudgetScenario(request)
            
            if not response.found:
                logger.warning(f"Scenario '{name}' not found: {response.error}")
                return None
            
            # Convert to dict for easier handling
            study_dict = {
                'name': response.study.name,
                'status': response.study.status,
                'trials': []
            }
            
            for trial in response.study.trials:
                trial_dict = {
                    'trial_id': trial.trial_id,
                    'budget': dict(trial.budget),
                    'values': list(trial.values),
                    'completed': trial.completed
                }
                study_dict['trials'].append(trial_dict)
            
            return study_dict
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error getting scenario: {e}")
            return None
    
    def get_best_trial(self, name: str) -> Optional[Dict[str, Any]]:
        """Get the best trial for a scenario"""
        try:
            request = budget_optimizer_pb2.GetBestTrialRequest(name=name)
            response = self.stub.GetBestTrial(request)
            
            if not response.found:
                logger.warning(f"Best trial for '{name}' not found: {response.error}")
                return None
            
            return {
                'trial_id': response.best_trial.trial.trial_id,
                'budget': dict(response.best_trial.trial.budget),
                'values': list(response.best_trial.trial.values),
                'objective_value': response.best_trial.objective_value,
                'total_trials': response.best_trial.total_trials
            }
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error getting best trial: {e}")
            return None
    
    def predict(self, budget: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """Make a revenue prediction"""
        try:
            # Create budget object
            budget_obj = budget_optimizer_pb2.Budget(
                olv=budget.get('olv', 0),
                paid_search=budget.get('paid_search', 0),
                print=budget.get('print', 0),
                radio=budget.get('radio', 0)
            )
            
            request = budget_optimizer_pb2.PredictRequest(budget=budget_obj)
            response = self.stub.Predict(request)
            
            return {
                'prediction': response.prediction,
                'currency': response.currency,
                'channel_contributions': dict(response.channel_contributions)
            }
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error making prediction: {e}")
            return None
    
    def stream_optimization(self, scenario_name: str, poll_interval: int = 5) -> Iterator[Dict[str, Any]]:
        """Stream optimization progress updates"""
        try:
            request = budget_optimizer_pb2.StreamOptimizationRequest(
                scenario_name=scenario_name,
                poll_interval_seconds=poll_interval
            )
            
            logger.info(f"Starting optimization stream for '{scenario_name}'")
            
            for update in self.stub.StreamOptimization(request):
                yield {
                    'scenario_name': update.scenario_name,
                    'current_trial': update.current_trial,
                    'total_trials': update.total_trials,
                    'best_value': update.best_value,
                    'best_budget': dict(update.best_budget),
                    'status': update.status,
                    'timestamp': update.timestamp.ToDatetime().isoformat()
                }
                
                # Break if completed or failed
                if update.status in ['completed', 'failed', 'error']:
                    break
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error streaming optimization: {e}")
    
    def delete_scenario(self, name: str) -> bool:
        """Delete a budget scenario"""
        try:
            request = budget_optimizer_pb2.DeleteBudgetScenarioRequest(name=name)
            response = self.stub.DeleteBudgetScenario(request)
            
            if response.success:
                logger.info(f"Deleted scenario: {response.message}")
                return True
            else:
                logger.error(f"Failed to delete scenario: {response.error}")
                return False
        
        except grpc.RpcError as e:
            logger.error(f"gRPC error deleting scenario: {e}")
            return False


# Example usage and demos
def demo_basic_operations():
    """Demonstrate basic operations"""
    client = BudgetOptimizerClient()
    
    try:
        # Connect to server
        client.connect()
        
        # Health check
        if not client.health_check():
            logger.error("Service is not healthy, exiting")
            return
        
        # List existing scenarios
        scenarios = client.list_scenarios()
        logger.info(f"Existing scenarios: {scenarios}")
        
        # Create a new scenario
        channel_budgets = {
            'olv': {'initial': 10, 'lower': 5, 'upper': 15},
            'paid_search': {'initial': 10, 'lower': 5, 'upper': 15},
            'print': {'initial': 10, 'lower': 5, 'upper': 15},
            'radio': {'initial': 10, 'lower': 5, 'upper': 15}
        }
        
        total_budget = {'initial': 40, 'lower': 40, 'upper': 45}
        
        scenario_name = f"demo_scenario_{int(time.time())}"
        success = client.create_budget_scenario(
            name=scenario_name,
            channel_budgets=channel_budgets,
            total_budget=total_budget,
            timeout=1,  # 1 minute for demo
            n_trials=10
        )
        
        if success:
            logger.info(f"Created scenario: {scenario_name}")
            
            # Wait a bit for optimization to start
            time.sleep(2)
            
            # Get scenario details
            scenario_details = client.get_scenario(scenario_name)
            if scenario_details:
                logger.info(f"Scenario details: {len(scenario_details['trials'])} trials")
            
            # Make a prediction
            test_budget = {'olv': 12, 'paid_search': 8, 'print': 10, 'radio': 10}
            prediction = client.predict(test_budget)
            if prediction:
                logger.info(f"Prediction: ${prediction['prediction']:,.2f}")
                logger.info(f"Contributions: {prediction['channel_contributions']}")
            
            # Clean up - delete the demo scenario
            client.delete_scenario(scenario_name)
    
    finally:
        client.disconnect()


def demo_streaming_optimization():
    """Demonstrate streaming optimization progress"""
    client = BudgetOptimizerClient()
    
    try:
        client.connect()
        
        if not client.health_check():
            logger.error("Service is not healthy, exiting")
            return
        
        # Create a scenario for streaming demo
        channel_budgets = {
            'olv': {'initial': 10, 'lower': 5, 'upper': 15},
            'paid_search': {'initial': 10, 'lower': 5, 'upper': 15},
            'print': {'initial': 10, 'lower': 5, 'upper': 15},
            'radio': {'initial': 10, 'lower': 5, 'upper': 15}
        }
        
        total_budget = {'initial': 40, 'lower': 40, 'upper': 45}
        scenario_name = f"streaming_demo_{int(time.time())}"
        
        success = client.create_budget_scenario(
            name=scenario_name,
            channel_budgets=channel_budgets,
            total_budget=total_budget,
            timeout=2,  # 2 minutes
            n_trials=50
        )
        
        if success:
            logger.info(f"Created scenario for streaming: {scenario_name}")
            
            # Stream optimization progress
            for update in client.stream_optimization(scenario_name, poll_interval=3):
                logger.info(f"Optimization update: {update}")
                
                if update['status'] in ['completed', 'failed']:
                    break
            
            # Get final best trial
            best_trial = client.get_best_trial(scenario_name)
            if best_trial:
                logger.info(f"Final best trial: {best_trial}")
            
            # Clean up
            client.delete_scenario(scenario_name)
    
    finally:
        client.disconnect()


async def demo_concurrent_operations():
    """Demonstrate concurrent operations"""
    client = BudgetOptimizerClient()
    
    try:
        client.connect()
        
        # Create multiple scenarios concurrently
        tasks = []
        for i in range(3):
            channel_budgets = {
                'olv': {'initial': 10 + i, 'lower': 5, 'upper': 15},
                'paid_search': {'initial': 10, 'lower': 5, 'upper': 15},
                'print': {'initial': 10, 'lower': 5, 'upper': 15},
                'radio': {'initial': 10, 'lower': 5, 'upper': 15}
            }
            
            total_budget = {'initial': 40 + i, 'lower': 40, 'upper': 45}
            scenario_name = f"concurrent_demo_{i}_{int(time.time())}"
            
            # Note: In a real async implementation, you'd use async gRPC
            success = client.create_budget_scenario(
                name=scenario_name,
                channel_budgets=channel_budgets,
                total_budget=total_budget,
                timeout=1,
                n_trials=10
            )
            
            if success:
                logger.info(f"Created concurrent scenario: {scenario_name}")
        
        # List all scenarios
        scenarios = client.list_scenarios()
        logger.info(f"All scenarios: {scenarios}")
        
        # Clean up concurrent scenarios
        for scenario in scenarios:
            if 'concurrent_demo' in scenario:
                client.delete_scenario(scenario)
    
    finally:
        client.disconnect()


if __name__ == '__main__':
    print("=== Budget Optimizer gRPC Client Demo ===\n")
    
    print("1. Basic Operations Demo")
    demo_basic_operations()
    print()
    
    print("2. Streaming Optimization Demo")
    demo_streaming_optimization()
    print()
    
    print("3. Concurrent Operations Demo")
    asyncio.run(demo_concurrent_operations())
    print()
    
    print("Demo completed!")