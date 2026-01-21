"""
YOLO Object Detection for Telegram Images - Alternative Implementation
Task 3 - Data Enrichment
"""

import os
import json
from pathlib import Path
from datetime import datetime
import logging
import cv2
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import warnings
warnings.filterwarnings('ignore')

# Try to import ultralytics, fallback to OpenCV if not available
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
    print("✅ Using ultralytics YOLO")
except ImportError:
    YOLO_AVAILABLE = False
    print("⚠️ ultralytics not available, using OpenCV DNN")

try:
    import torch
    TORCH_AVAILABLE = True
except:
    TORCH_AVAILABLE = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YOLODetector:
    """YOLO Object Detector for Telegram Images"""
    
    def __init__(self, model_name='yolov8n.pt', use_opencv_fallback=False):
        """Initialize YOLO detector"""
        self.use_opencv = use_opencv_fallback or not YOLO_AVAILABLE
        
        if self.use_opencv:
            logger.info("Using OpenCV DNN for object detection")
            self.model = self.load_opencv_model()
            self.classes = self.load_coco_classes()
        else:
            logger.info(f"Loading YOLO model: {model_name}")
            try:
                self.model = YOLO(model_name)
                logger.info("✅ YOLO model loaded successfully")
                self.classes = self.model.names
            except Exception as e:
                logger.error(f"❌ Failed to load YOLO model: {e}")
                logger.info("Falling back to OpenCV DNN")
                self.use_opencv = True
                self.model = self.load_opencv_model()
                self.classes = self.load_coco_classes()
        
        # Object categories for classification
        self.person_objects = ['person']
        self.product_objects = ['bottle', 'cup', 'bowl', 'handbag', 'backpack', 
                               'cell phone', 'clock', 'vase', 'scissors', 'book',
                               'chair', 'couch', 'potted plant', 'dining table']
        self.medical_objects = ['bottle', 'vase']
        
    def load_opencv_model(self):
        """Load YOLO model using OpenCV DNN"""
        # Download YOLOv3-tiny model files
        model_url = "https://pjreddie.com/media/files/yolov3-tiny.weights"
        config_url = "https://github.com/pjreddie/darknet/blob/master/cfg/yolov3-tiny.cfg?raw=true"
        
        model_path = "models/yolov3-tiny.weights"
        config_path = "models/yolov3-tiny.cfg"
        
        # Create models directory
        os.makedirs("models", exist_ok=True)
        
        # Download files if they don't exist
        import urllib.request
        
        if not os.path.exists(model_path):
            logger.info("Downloading YOLOv3-tiny weights...")
            urllib.request.urlretrieve(model_url, model_path)
            
        if not os.path.exists(config_path):
            logger.info("Downloading YOLOv3-tiny config...")
            urllib.request.urlretrieve(config_url, config_path)
        
        # Load network
        net = cv2.dnn.readNet(model_path, config_path)
        net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
        net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
        
        return net
    
    def load_coco_classes(self):
        """Load COCO class names"""
        coco_classes = [
            'person', 'bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck',
            'boat', 'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench',
            'bird', 'cat', 'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra',
            'giraffe', 'backpack', 'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee',
            'skis', 'snowboard', 'sports ball', 'kite', 'baseball bat', 'baseball glove',
            'skateboard', 'surfboard', 'tennis racket', 'bottle', 'wine glass', 'cup',
            'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple', 'sandwich', 'orange',
            'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair', 'couch',
            'potted plant', 'bed', 'dining table', 'toilet', 'tv', 'laptop', 'mouse',
            'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink',
            'refrigerator', 'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier',
            'toothbrush'
        ]
        return {i: name for i, name in enumerate(coco_classes)}
    
    def detect_with_opencv(self, image_path: str):
        """Run object detection using OpenCV DNN"""
        image = cv2.imread(image_path)
        if image is None:
            return None, None
        
        height, width = image.shape[:2]
        
        # Prepare input blob
        blob = cv2.dnn.blobFromImage(image, 1/255.0, (416, 416), swapRB=True, crop=False)
        self.model.setInput(blob)
        
        # Get output layer names
        layer_names = self.model.getLayerNames()
        output_layers = [layer_names[i - 1] for i in self.model.getUnconnectedOutLayers()]
        
        # Run inference
        outputs = self.model.forward(output_layers)
        
        # Process detections
        detections = []
        confidence_scores = []
        conf_threshold = 0.25
        
        for output in outputs:
            for detection in output:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                
                if confidence > conf_threshold:
                    class_name = self.classes.get(class_id, f"class_{class_id}")
                    detections.append(class_name)
                    confidence_scores.append(float(confidence))
        
        return detections, confidence_scores
    
    def detect_image(self, image_path: str) -> Dict[str, Any]:
        """Run object detection on a single image"""
        if not os.path.exists(image_path):
            logger.warning(f"Image not found: {image_path}")
            return None
        
        try:
            if self.use_opencv:
                detections, confidence_scores = self.detect_with_opencv(image_path)
            else:
                # Run inference with ultralytics
                results = self.model(image_path, conf=0.25)
                detections = []
                confidence_scores = []
                
                for result in results:
                    boxes = result.boxes
                    if boxes is not None:
                        for box in boxes:
                            cls_id = int(box.cls[0])
                            conf = float(box.conf[0])
                            class_name = self.classes[cls_id]
                            detections.append(class_name)
                            confidence_scores.append(conf)
            
            # Classify image based on detected objects
            image_category = self.classify_image(detections) if detections else 'no_detection'
            
            return {
                'image_path': image_path,
                'detected_objects': detections if detections else [],
                'confidence_scores': confidence_scores if confidence_scores else [],
                'detection_count': len(detections) if detections else 0,
                'image_category': image_category,
                'has_person': any(obj in self.person_objects for obj in detections) if detections else False,
                'has_product': any(obj in self.product_objects for obj in detections) if detections else False,
                'detection_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return None
    
    def classify_image(self, detections: List[str]) -> str:
        """Classify image based on detected objects"""
        if not detections:
            return 'no_detection'
        
        has_person = any(obj in self.person_objects for obj in detections)
        has_product = any(obj in self.product_objects for obj in detections)
        has_medical = any(obj in self.medical_objects for obj in detections)
        
        if has_person and has_product:
            return 'promotional'
        elif has_product and not has_person:
            return 'product_display'
        elif has_person and not has_product:
            return 'lifestyle'
        elif has_medical:
            return 'medical_product'
        else:
            return 'other_content'
    
    def process_directory(self, image_dir: str, output_csv: str = None) -> List[Dict[str, Any]]:
        """Process all images in a directory"""
        logger.info(f"Processing images in: {image_dir}")
        
        # Find all image files
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp']
        image_files = []
        
        for ext in image_extensions:
            image_files.extend(Path(image_dir).rglob(f'*{ext}'))
            image_files.extend(Path(image_dir).rglob(f'*{ext.upper()}'))
        
        logger.info(f"Found {len(image_files)} image files")
        
        if not image_files:
            logger.warning("No image files found")
            return []
        
        results = []
        
        for i, img_path in enumerate(image_files, 1):
            if i % 10 == 0:
                logger.info(f"Processing image {i}/{len(image_files)}...")
            
            result = self.detect_image(str(img_path))
            if result:
                # Extract channel name from path
                try:
                    parts = str(img_path).split(os.sep)
                    if 'images' in parts:
                        idx = parts.index('images')
                        if idx + 1 < len(parts):
                            result['channel_name'] = parts[idx + 1]
                except:
                    result['channel_name'] = 'unknown'
                
                results.append(result)
        
        # Save results
        if output_csv and results:
            self.save_results(results, output_csv)
        
        logger.info(f"✅ Processed {len(results)} images")
        return results
    
    def save_results(self, results: List[Dict[str, Any]], output_path: str):
        """Save detection results to CSV"""
        # ... [keep the same save_results method from original] ...
        pass
    
    def analyze_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze detection results"""
        # ... [keep the same analyze_results method] ...
        pass
    
    def generate_report(self, results: List[Dict[str, Any]], report_path: str = None):
        """Generate analysis report"""
        # ... [keep the same generate_report method] ...
        pass

def main():
    """Main function for Task 3"""
    print("\n" + "="*60)
    print("TASK 3 - Data Enrichment with Object Detection")
    print("="*60)
    
    # Configuration
    IMAGE_DIR = "data/raw/images"
    OUTPUT_DIR = "data/processed/yolo"
    OUTPUT_CSV = os.path.join(OUTPUT_DIR, "detection_results.csv")
    REPORT_PATH = os.path.join(OUTPUT_DIR, "yolo_analysis_report.txt")
    
    # Create directories
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Check if images exist
    if not os.path.exists(IMAGE_DIR):
        print(f"❌ Image directory not found: {IMAGE_DIR}")
        print(f"Please run image scraping first (Task 2)")
        return
    
    # Initialize detector
    try:
        detector = YOLODetector(use_opencv_fallback=True)  # Force OpenCV for now
    except Exception as e:
        print(f"❌ Failed to initialize detector: {e}")
        print("\nTrying alternative approach...")
        return
    
    # Process images
    print(f"\n📁 Processing images from: {IMAGE_DIR}")
    results = detector.process_directory(IMAGE_DIR, OUTPUT_CSV)
    
    if results:
        # Generate report
        detector.generate_report(results, REPORT_PATH)
        
        # Load results into database
        print("\n💾 Loading results into database...")
        load_yolo_to_database(results)
        
        print("\n" + "="*60)
        print("✅ TASK 3 COMPLETED SUCCESSFULLY!")
        print("="*60)
    else:
        print("\n⚠️ No images processed.")

def load_yolo_to_database(results: List[Dict[str, Any]]):
    """Load detection results into SQLite database"""
    import sqlite3
    
    db_path = "data/medical_warehouse.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table for YOLO results
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS yolo_detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_path TEXT NOT NULL,
            channel_name TEXT,
            detected_objects TEXT,
            confidence_scores TEXT,
            detection_count INTEGER,
            image_category TEXT,
            has_person BOOLEAN,
            has_product BOOLEAN,
            detection_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Insert results
        for result in results:
            cursor.execute('''
                INSERT INTO yolo_detections 
                (image_path, channel_name, detected_objects, 
                 confidence_scores, detection_count, image_category,
                 has_person, has_product, detection_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result['image_path'],
                result.get('channel_name'),
                ','.join(result['detected_objects']),
                ','.join([str(round(s, 3)) for s in result['confidence_scores']]),
                result['detection_count'],
                result['image_category'],
                result['has_person'],
                result['has_product'],
                result['detection_time']
            ))
        
        conn.commit()
        conn.close()
        print("✅ Detection results loaded into database")
        
    except Exception as e:
        print(f"❌ Error loading to database: {e}")

if __name__ == "__main__":
    main()