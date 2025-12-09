# execution_summary.py

class ExecutionSummary:
    def __init__(self):
        # Variables to store log messages
        self.logs = []
        self.success = []
        self.failure = []
        self.failure_details = []

    def log(self, message):
        """Add a general log message."""
        self.logs.append(message)

    def add_success(self, message):
        """Add a success message."""
        self.success.append(message)
        self.logs.append(f"SUCCESS: {message}")  # Log the success message

    def add_failure(self, message, details):
        """Add a failure message with details."""
        self.failure.append(message)
        self.failure_details.append(details)
        self.logs.append(f"FAILURE: {message} - {details}")  # Log the failure message
        
    def get_success_count(self):
        """Return the count of success messages."""
        return len(self.success)

    def get_failure_count(self):
        """Return the count of failure messages."""
        return len(self.failure)
    
    def save_logs_to_file(self, filename):
        """Save all logs to a text file."""
        with open(filename, 'w') as f:
            f.write("\n".join(self.logs))

    def get_summary(self):
        """Compile all logs into a summary text."""
        summary = "Execution Summary:\n\n"
        
        summary += f"Total Successes: {self.get_success_count()}\n"
        summary += f"Total Failures: {self.get_failure_count()}\n\n"
        
        summary += "Success:\n" + "\n".join(self.success) + "\n\n"
        summary += "Failures:\n" + "\n".join(self.failure) + "\n\n"
        summary += "Failure Details:\n" + "\n".join(self.failure_details) + "\n"
        
        return summary

# Create a global instance
summary = ExecutionSummary()
