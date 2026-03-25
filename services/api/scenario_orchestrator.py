"""
Scenario orchestrator - starts/stops scenario Python processes
"""
import subprocess
import os
import signal
from typing import Optional, Dict
from pathlib import Path
import psutil
import sys


class ScenarioOrchestrator:
    def __init__(self):
        self.running_scenarios: Dict[str, Dict] = {}
        self.base_path = Path(__file__).parent.parent / "scenarios"
        
        # Get Python executable from virtual environment
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            # We're in a virtual environment
            self.python_exe = sys.executable
        else:
            # Fallback
            self.python_exe = "python"
        
        print(f"[ORCHESTRATOR] Using Python: {self.python_exe}")
        
    def _get_env_vars(self) -> Dict[str, str]:
        """Get environment variables for scenario processes"""
        env = os.environ.copy()
        # Ensure Kafka brokers is set
        if "KAFKA_BROKERS" not in env:
            env["KAFKA_BROKERS"] = "localhost:19092"
        return env
        
    def get_status(self, scenario_id: str) -> Dict:
        """Get status of a scenario"""
        if scenario_id not in self.running_scenarios:
            return {"status": "stopped", "processes": []}
        
        scenario_info = self.running_scenarios[scenario_id]
        # Check if processes are still alive
        active_processes = []
        for proc_info in scenario_info["processes"]:
            try:
                proc = psutil.Process(proc_info["pid"])
                if proc.is_running():
                    active_processes.append({
                        "name": proc_info["name"],
                        "pid": proc_info["pid"],
                        "status": "running"
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        if active_processes:
            return {
                "status": "running",
                "processes": active_processes,
                "started_at": scenario_info.get("started_at")
            }
        else:
            # Cleanup if processes died
            del self.running_scenarios[scenario_id]
            return {"status": "stopped", "processes": []}
    
    def start_scenario(self, scenario_id: str) -> Dict:
        """Start a scenario's Python processes"""
        # Check if already running
        status = self.get_status(scenario_id)
        if status["status"] == "running":
            return {"success": False, "message": "Scenario already running", "status": status}
        
        scenario_handlers = {
            "simple-pipeline": self._start_simple_pipeline,
            "fan-out": self._start_fan_out,
            "fan-in": self._start_fan_in,
            "complex-dag": self._start_complex_dag,
        }
        
        if scenario_id in scenario_handlers:
            return scenario_handlers[scenario_id]()
        else:
            return {"success": False, "message": f"Unknown scenario: {scenario_id}"}
    
    def stop_scenario(self, scenario_id: str) -> Dict:
        """Stop a scenario's Python processes"""
        if scenario_id not in self.running_scenarios:
            return {"success": True, "message": "Scenario not running"}
        
        scenario_info = self.running_scenarios[scenario_id]
        stopped = []
        
        for proc_info in scenario_info["processes"]:
            try:
                proc = psutil.Process(proc_info["pid"])
                proc.terminate()  # Graceful termination
                try:
                    proc.wait(timeout=3)
                except psutil.TimeoutExpired:
                    proc.kill()  # Force kill if doesn't respond
                stopped.append(proc_info["name"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        del self.running_scenarios[scenario_id]
        
        return {
            "success": True,
            "message": f"Stopped {len(stopped)} processes",
            "stopped": stopped
        }
    
    def _start_generic_scenario(self, scenario_id: str, process_configs: list) -> Dict:
        """Generic method to start any scenario with given process configs"""
        import time
        scenario_path = self.base_path / scenario_id.replace("-", "_")
        
        if not scenario_path.exists():
            return {"success": False, "message": f"Scenario path not found: {scenario_path}"}
        
        processes = []
        env = self._get_env_vars()
        
        for config in process_configs:
            if "delay" in config:
                time.sleep(config["delay"])
            
            try:
                script_path = scenario_path / config["file"]
                if not script_path.exists():
                    raise FileNotFoundError(f"Script not found: {script_path}")
                
                print(f"[ORCHESTRATOR] Starting {config['name']} at {script_path}")
                
                # Start process WITHOUT capturing output
                proc = subprocess.Popen(
                    [self.python_exe, str(script_path)],
                    cwd=str(scenario_path),
                    env=env,
                    stdout=None,
                    stderr=None,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
                )
                
                # Give it a moment to start and check if it's still alive
                time.sleep(1)
                if proc.poll() is not None:
                    raise RuntimeError(f"Process exited immediately with code {proc.returncode}")
                
                processes.append({
                    "name": config["name"],
                    "pid": proc.pid,
                    "file": config["file"]
                })
                print(f"[ORCHESTRATOR] Started {config['name']} (PID: {proc.pid})")
                
            except Exception as e:
                print(f"[ORCHESTRATOR] ERROR starting {config['name']}: {e}")
                # Cleanup on error
                for p in processes:
                    try:
                        psutil.Process(p["pid"]).terminate()
                    except:
                        pass
                return {"success": False, "message": f"Failed to start {config['name']}: {str(e)}"}
        
        self.running_scenarios[scenario_id] = {
            "processes": processes,
            "started_at": time.time()
        }
        
        return {
            "success": True,
            "message": f"Started {scenario_id}",
            "processes": [p["name"] for p in processes]
        }
    
    def _start_simple_pipeline(self) -> Dict:
        """Start Simple Pipeline: Producer -> Transform -> Consumer"""
        return self._start_generic_scenario("simple-pipeline", [
            {"name": "producer", "file": "producer.py"},
            {"name": "transform", "file": "transform.py", "delay": 2},
            {"name": "consumer", "file": "consumer.py", "delay": 2}
        ])
    
    def _start_fan_out(self) -> Dict:
        """Start Fan-Out Pattern: 1 Producer -> Specialized Processors"""
        return self._start_generic_scenario("fan-out", [
            {"name": "producer", "file": "producer.py"},
            {"name": "filter_processor", "file": "filter_processor.py", "delay": 2},
            {"name": "enrich_processor", "file": "enrich_processor.py", "delay": 2},
            {"name": "aggregate_processor", "file": "aggregate_processor.py", "delay": 2}
        ])
    
    def _start_fan_in(self) -> Dict:
        """Start Fan-In Pattern: 3 Sources -> 1 Aggregator"""
        return self._start_generic_scenario("fan-in", [
            {"name": "source_a", "file": "source_a.py"},
            {"name": "source_b", "file": "source_b.py", "delay": 1},
            {"name": "source_c", "file": "source_c.py", "delay": 1},
            {"name": "aggregator", "file": "aggregator.py", "delay": 3}
        ])
    
    def _start_complex_dag(self) -> Dict:
        """Start Complex DAG: Multi-stage processing with joins"""
        return self._start_generic_scenario("complex-dag", [
            {"name": "orders_stream", "file": "orders_stream.py"},
            {"name": "users_stream", "file": "users_stream.py", "delay": 1},
            {"name": "enricher", "file": "enricher.py", "delay": 3},
            {"name": "fraud_check", "file": "fraud_check.py", "delay": 2},
            {"name": "risk_scorer", "file": "risk_scorer.py", "delay": 2},
            {"name": "aggregator", "file": "aggregator.py", "delay": 2},
            {"name": "output", "file": "output.py", "delay": 2}
        ])
    
    def get_all_status(self) -> Dict[str, Dict]:
        """Get status of all scenarios"""
        return {
            "simple-pipeline": self.get_status("simple-pipeline"),
            "fan-out": self.get_status("fan-out"),
            "fan-in": self.get_status("fan-in"),
            "complex-dag": self.get_status("complex-dag"),
        }



# Global orchestrator instance
_orchestrator: Optional[ScenarioOrchestrator] = None

def get_orchestrator() -> ScenarioOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = ScenarioOrchestrator()
    return _orchestrator
