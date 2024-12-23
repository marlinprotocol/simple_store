import requests
import json

BASE_URL = "http://localhost:8080"

def test_store_payload():
    """Test the store_payload function by sending a payload to the server."""
    url = f"{BASE_URL}/store"
    payload = {"payload": "This is a test payload for S3 storage."}
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        print("Store Payload Response:", data)
        return data["id"]  # Return the ID of the stored object
    except requests.exceptions.RequestException as e:
        print("Error storing payload:", e)
        return None

def test_get_payload(payload_id):
    """Test the get_payload function by retrieving a payload from the server."""
    url = f"{BASE_URL}/{payload_id}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        print("Get Payload Response:", data)
    except requests.exceptions.RequestException as e:
        print("Error retrieving payload:", e)

def main():
    print("Testing Actix Web API...")
    
    # Step 1: Store payload
    payload_id = test_store_payload()
    if not payload_id:
        print("Failed to store payload. Exiting...")
        return
    
    # Step 2: Retrieve payload
    test_get_payload(payload_id)

if __name__ == "__main__":
    main()