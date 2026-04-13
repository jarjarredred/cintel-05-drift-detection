# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## Custom Project

### Dataset
The original dataset was used tracking requests, errors and latency from a reference and a current file

### Signals
The requests are the volume signals. Errors are reliability signals. Latency is a performance signal.

### Experiments
In the original script, we just flagged if drift happened. In this modification, I added a "Drift Severity Score". Instead of just a true/false flag, I calculated the percentage change relative to the baseline. This helps prioritize which metric is failing most

### Results
This allows us to easily access which of our system metrics is deviating most from the norm.

### Interpretation
We could trigger a warning if the errors_pct_change is greater than 50%, for example.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)
