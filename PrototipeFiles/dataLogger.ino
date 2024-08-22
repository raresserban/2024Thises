// Libraryes
#include <esp_now.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <NTPClient.h>
#include <WiFiUdp.h>
#include "MPU6050_6Axis_MotionApps20.h"
#include "Wire.h"
#include <SD.h>
#include <set>
#include <HTTPClient.h>
#include "FS.h"
#include "SPI.h"

// Buttons and leds
#define FILE_BUTTON_PIN 13
#define RED_LED_PIN 12
#define SWITCH_PIN 32
#define GREEN_LED_PIN 33
#define WIFI_LED 2

File dataFile;
bool recording = false;
bool transmiting = false;

// ID s and passwords (Make a secret in production)
const char* ssid = SECRET_SSID;
const char* password = SECRET_PASSWORD;
const char* azureBlobUri = "https://___.blob.core.windows.net/___";
const char* sastoken = SECRET_TOKEN;

// NTP Client time variables

WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "pool.ntp.org", 2 * 3600, 60000);
esp_sleep_wakeup_cause_t wakeup_reason;

unsigned long prevmillis = 0;
unsigned long lastSyncTime = 0;
unsigned long lastMicros = 0;
unsigned long starttime = 0;


uint8_t fifoBuffer[64];
MPU6050 mpu6500(0x68, &Wire);
String mpuVersion = "";

// Function declarations
void setupESP_NOW();
void connectToWiFi();
void syncTime();
void handleSwitch();
void handleButton();
void startRecording();
void stopRecording();
void innitMPU6500();
void recordData6500();
String getTimeStamp();
template<typename WireType = TwoWire>
uint8_t readByte(uint8_t address, uint8_t subAddress, WireType& wire = Wire);
void OnDataRecv(const uint8_t* mac, const uint8_t* incomingData, int len);
String getBlobFiles();
float getTimeVector();
void startTransmission();
void transmitFiles(File dir);
bool uploadFile(File file);

typedef struct struct_message {
  int16_t ax1, ay1, az1, gx1, gy1, gz1;
  float qw1, qx1, qy1, qz1;
  float wax1, way1, waz1, wgx1, wgy1, wgz1;
  float temp1;
  int button1, button2, button3;
} struct_message;

struct_message wristData;

// Setup script
void setup() {
  Serial.begin(115200);

  // Setup Wires for both MPU I2C communication
  Wire.begin(27, 26, 400000);

  pinMode(FILE_BUTTON_PIN, INPUT_PULLUP);
  pinMode(SWITCH_PIN, INPUT_PULLUP);

  ledcAttach(WIFI_LED, 12000, 8);
  pinMode(RED_LED_PIN, OUTPUT);
  pinMode(GREEN_LED_PIN, OUTPUT);

  // Connect to Wi-Fi
  connectToWiFi();

  // Initialize NTP Client
  timeClient.begin();
  syncTime();

  setupESP_NOW();

  // Find if we use MPU9250 or MPU6500 to initialize the right modules
  byte ca = readByte(0x68, 0x75, Wire);
  if (ca == 0x70) {
    Serial.println("Chest using MPU6500");
    mpuVersion = "MPU6500";
    innitMPU6500();
  } else {
    Serial.println("Chest MPU ERROR");
    for (int i = 0; i < 40; i++) {
      ledcWrite(WIFI_LED, 255);
      delay(62);
      ledcWrite(WIFI_LED, 0);
      delay(62);
    }
  }

  // Initialize SD card with retryes
  unsigned long sdinittime = millis();
  while (!SD.begin(5)) {
    Serial.println("SD mount failed");
    for (int i = 0; i < 8; i++) {
      ledcWrite(WIFI_LED, 255);
      delay(31);
      ledcWrite(WIFI_LED, 0);
      delay(31);
    }
    if (millis() - sdinittime > 10000) {
      Serial.println("SD mount timed out");
      ledcWrite(WIFI_LED, 255);
      delay(1000);
      ledcWrite(WIFI_LED, 0);
      break;
    }
  }
  uint8_t cardType = SD.cardType();
  if (cardType == CARD_NONE) {
    Serial.println(" No SD card attached");
    return;
  }

  wakeup_reason = esp_sleep_get_wakeup_cause();

  // Configure deep sleep on Switch and Transfer button
  esp_sleep_enable_ext1_wakeup(0x100002000, ESP_EXT1_WAKEUP_ALL_LOW);
  esp_sleep_enable_ext0_wakeup(GPIO_NUM_32, 1);
}

void loop() {
  // 10ms between datapoints
  unsigned long currentmillis = millis();
  if (currentmillis - prevmillis >= 10) {
    prevmillis = currentmillis;

    // Handle if we are recording or transfering
    handleSwitch();
    handleButton();

    // Handle the recording based on the sensors
    if (recording) {
      recordData6500();
    }
  }
}

// Initialize MPU6500
void innitMPU6500() {

  mpu6500.initialize();
  mpu6500.dmpInitialize();

  mpu6500.setXAccelOffset(2743);
  mpu6500.setYAccelOffset(5476);
  mpu6500.setZAccelOffset(13567);
  mpu6500.setXGyroOffset(-61);
  mpu6500.setYGyroOffset(-47);
  mpu6500.setZGyroOffset(-79);
  mpu6500.setFullScaleAccelRange(MPU6050_ACCEL_FS_8);
  mpu6500.setFullScaleGyroRange(MPU6050_GYRO_FS_1000);

  mpu6500.setDMPEnabled(true);
}

void connectToWiFi() {
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    for (int i = 0; i < 4; i++) {
      ledcWrite(WIFI_LED, 255);
      delay(125);
      ledcWrite(WIFI_LED, 0);
      delay(125);
    }
    Serial.println("Connecting to WiFi...");
  }
  Serial.println("Connected to WiFi");
}

void setupESP_NOW() {

  WiFi.mode(WIFI_AP_STA);

  while (esp_now_init() != ESP_OK) {
    for (int i = 0; i < 4; i++) {
      ledcWrite(WIFI_LED, 255);
      delay(125);
      ledcWrite(WIFI_LED, 0);
      delay(125);
    }
    Serial.println("Connecting to ESP-NOW...");
  }
  Serial.println("Connected to ESP-NOW");

  esp_now_register_recv_cb(esp_now_recv_cb_t(OnDataRecv));
}


// Function to get the bits recognizing the MPU type
template<typename WireType>
uint8_t readByte(uint8_t address, uint8_t subAddress, WireType& wire) {
  uint8_t data = 0;
  wire.beginTransmission(address);
  wire.write(subAddress);
  wire.endTransmission(false);
  wire.requestFrom(address, (size_t)1);
  if (wire.available()) data = wire.read();
  return data;
}

// request to sync the time to eliminate drift
void syncTime() {
  timeClient.update();
  lastSyncTime = timeClient.getEpochTime();
  lastMicros = micros();
}

// Handle the recording switch
void handleSwitch() {
  if (digitalRead(SWITCH_PIN) == HIGH) {
    if (!recording) {
      starttime = millis();
      startRecording();
    }
  } else {
    if (recording) {
      stopRecording();

      delay(500);

      Serial.println("Going to sleep...");
      esp_deep_sleep_start();
    }
  }
}
// Handle the transmiting button
void handleButton() {
  if (!recording && !transmiting) {
    if ((digitalRead(FILE_BUTTON_PIN) == LOW) || (wakeup_reason == ESP_SLEEP_WAKEUP_EXT1) ) {
      startTransmission();
      wakeup_reason = ESP_SLEEP_WAKEUP_UNDEFINED;
    } else if (digitalRead(FILE_BUTTON_PIN) == HIGH) {
      Serial.println("Going to sleep...");
      esp_deep_sleep_start();
    }
  }
}

// Create a file with metadata in the name for later extraction
void startRecording() {

  recording = true;

  digitalWrite(GREEN_LED_PIN, HIGH);

  String fileName = "/" + getTimeStamp() + "_" + mpuVersion + "_LICENTA_ser.csv";
  dataFile = SD.open(fileName.c_str(), FILE_WRITE);
  if (dataFile) {
    dataFile.println("timevector,ax1,ay1,az1,gx1,gy1,gz1,qx1,qy1,qz1,qw1,wax1,way1,waz1,wgx1,wgy1,wgz1,temp1,ax2,ay2,az2,gx2,gy2,gz2,qx2,qy2,qz2,qw2,wax2,way2,waz2,wgx2,wgy2,wgz2,temp2,button1,button2,button3");
    Serial.println("Recording started: " + fileName);
  } else {
    Serial.println("Failed to create file");
  }
}

void stopRecording() {
  recording = false;

  digitalWrite(GREEN_LED_PIN, LOW);

  if (dataFile) {
    dataFile.close();
    Serial.println("Recording stopped.");
  }
}

// Handle missing data with Null strings
String convertToString(float value) {
  if (isnan(value)) {
    return "NULL";
  } else {
    return String(value, 6);
  }
}
String convertToString(double value) {
  if (isnan(value)) {
    return "NULL";
  } else {
    return String(value, 6);
  }
}

String convertToString(int value) {
  return String(value);
}

void OnDataRecv(const uint8_t* mac, const uint8_t* incomingData, int len) {
  memcpy(&wristData, incomingData, sizeof(wristData));
}


// Record the data based of the MPU6500
void recordData6500() {

  Quaternion q;
  VectorInt16 aa, aaWorld;
  VectorInt16 gyro, ggWorld;

  if (mpu6500.dmpGetCurrentFIFOPacket(fifoBuffer)) {  // Get the Latest packet

    mpu6500.dmpGetQuaternion(&q, fifoBuffer);
    mpu6500.dmpGetAccel(&aa, fifoBuffer);
    mpu6500.dmpConvertToWorldFrame(&aaWorld, &aa, &q);
    mpu6500.dmpGetGyro(&gyro, fifoBuffer);
    mpu6500.dmpConvertToWorldFrame(&ggWorld, &gyro, &q);
  }

  String dataString;
  dataString.reserve(512);
  dataString = convertToString(getTimeVector()) + "," + convertToString(wristData.ax1) + "," + convertToString(wristData.ay1) + "," + convertToString(wristData.az1) + "," + convertToString(wristData.gx1) + "," + convertToString(wristData.gy1) + "," + convertToString(wristData.gz1) + "," + convertToString(wristData.qx1) + "," + convertToString(wristData.qy1) + "," + convertToString(wristData.qz1) + "," + convertToString(wristData.qw1) + "," + convertToString(wristData.wax1) + "," + convertToString(wristData.way1) + "," + convertToString(wristData.waz1) + "," + convertToString(wristData.wgx1) + "," + convertToString(wristData.wgy1) + "," + convertToString(wristData.wgz1) + "," + convertToString(wristData.temp1) + "," + convertToString(aa.x) + "," + convertToString(aa.y) + "," + convertToString(aa.z) + "," + convertToString(gyro.x) + "," + convertToString(gyro.y) + "," + convertToString(gyro.z) + "," + convertToString(q.x) + "," + convertToString(q.y) + "," + convertToString(q.z) + "," + convertToString(q.w) + "," + convertToString(aaWorld.x) + "," + convertToString(aaWorld.y) + "," + convertToString(aaWorld.z) + "," + convertToString(ggWorld.x) + "," + convertToString(ggWorld.y) + "," + convertToString(ggWorld.z) + "," + convertToString(mpu6500.getTemperature() / 340.00 + 36.53) + "," + convertToString(wristData.button1) + "," + convertToString(wristData.button2) + "," + convertToString(wristData.button3);
  if (dataFile) {
    dataFile.println(dataString);
    dataFile.flush();
  } else {
    Serial.println("Error writing to file");
  }
}

// Get the exact time with the format appropriate for a file
String getTimeStamp() {
  unsigned long currentMicros = micros();
  unsigned long elapsedMicros = currentMicros - lastMicros;
  time_t currentTimeSec = lastSyncTime + elapsedMicros / 1000000;
  unsigned long curentTimeMicros = elapsedMicros % 1000000;

  struct tm timeinfo;
  struct tm* timeinfoPtr = gmtime_r(&currentTimeSec, &timeinfo);

  if (timeinfoPtr == NULL) {
    Serial.println('gmtime_r failed');
  }

  char timestamp[32];
  int n = snprintf(timestamp, sizeof(timestamp), "%04d%02d%02d-%02d%02d%02d_%06lu", timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday, timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec, curentTimeMicros);

  if (n >= sizeof(timestamp)) {
    Serial.print("Buffer Overflow");
  }
  return String(timestamp);
}

// Get the vector since the start of the recording
float getTimeVector() {
  unsigned long timeSinceTrigger = millis() - starttime;
  return timeSinceTrigger / 1000.0;
}

// Transmit all the files recorded
void startTransmission() {
  transmiting = true;
  digitalWrite(RED_LED_PIN, HIGH);

  if (WiFi.status() == WL_CONNECTED) {
    File root = SD.open("/");
    transmitFiles(root);
    root.close();
  } else {
    Serial.println("Failed to connect to WiFi");
  }
}

// Transmit and inform of the success
void transmitFiles(File dir) {
  while (true) {
    File entry = dir.openNextFile();
    if (!entry) {
      digitalWrite(RED_LED_PIN, LOW);
      transmiting = false;
      break;
    }

    if (!entry.isDirectory()) {
      Serial.println("Uploading: " + String(entry.name()) + " Size: " + String(entry.size() / (1024.0 * 1024.0)));
      bool success = false;
      if (entry.size() == 0.0) {
        success = true;
      } else {
        success = uploadFile(entry);
      }
      if (success) {
        if (SD.remove("/" + String(entry.name()))) {
          Serial.println("Removed: " + String(entry.name()));
        } else {
          Serial.println("Failed to remove: " + String(entry.name()));
        }
        entry.close();
      } else {
        Serial.println("Failed to upload: " + String(entry.name()));
      }
    }
  }
}


// Send to blob storage with a stream straight form the sd card
bool uploadFile(File file) {
  HTTPClient http;
  String url = String(azureBlobUri) + "/data/" + file.name() + sastoken;
  http.begin(url);

  http.addHeader("x-ms-blob-type", "BlockBlob");
  http.addHeader("Content-Type", "application/octet-stream");

  int httpResponseCode = http.sendRequest("PUT", &file, file.size());

  if (httpResponseCode == 201 || httpResponseCode == 200) {
    Serial.println("File uploaded successfully: " + String(file.name()));
    http.end();
    return true;
  } else {
    Serial.println("Failed to upload file: " + String(url));
    Serial.println("HTTP Response code: " + String(httpResponseCode));
    http.end();
    return false;
  }
}
