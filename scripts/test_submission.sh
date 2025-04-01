#!/bin/bash

# Generate test data if not exists
if [ ! -d "test_data/level_4" ]; then
  echo "Generating test data..."
  docker-compose build data-generator
  docker-compose run data-generator python generate_data.py
fi

# Test each level
for level in {1..4}; do
  echo "Testing Level $level submission..."
  
  # Run pipeline with output logging
  echo -e "\n=== PIPELINE OUTPUT ==="
  docker-compose run evaluator \
    python pipeline.py /data/test_data/level_$level /results/level_$level
  
  # Evaluate results with output logging
  echo -e "\n=== EVALUATION RESULTS ==="
  docker-compose run --entrypoint "python -u" evaluator \
    evaluate.py /data/test_data/level_$level /data/results/level_$level | tee /dev/stderr
  
  # Save evaluation
  mkdir -p submissions/results
  chmod -R 777 submissions
  docker cp $(docker ps -lq):/results/evaluation.json submissions/results/level_$level.json
  echo -e "\nSaved results to submissions/results/level_$level.json"
done

# Generate final report
echo "Generating submission report..."
mkdir -p submissions
docker-compose run evaluator python -c "
import json, glob, os
os.makedirs('/data/submissions', exist_ok=True)
results = {}
for f in glob.glob('/data/submissions/results/*.json'):
    with open(f) as infile:
        results.update(json.load(infile))
with open('/data/submissions/final_report.json', 'w') as outfile:
    json.dump(results, outfile, indent=2)
"

echo "Submission test complete!"
echo "Results saved to submissions/final_report.json"