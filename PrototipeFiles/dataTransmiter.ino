// Libraryes
#include <esp_now.h>
#include <esp_wifi.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include "MPU6050_6Axis_MotionApps20.h"
#include "Wire.h"
#include <HTTPClient.h>
#include "FS.h"
#include "SPI.h"

// Buttons and leds
#define BUTTON_PIN_1 32
#define BUTTON_PIN_2 33
#define BUTTON_PIN_3 25
#define SLEEP_PIN 19
#define WIFI_LED 2

// ID s and passwords (Make a secret in production)
uint8_t broadcastAddress[] = { 0x30, 0xAE, 0xA4, 0x41, 0x69, 0x38 };
constexpr char WIFI_SSID[] = SECRET_SSID;


uint8_t fifoBuffer[64];
unsigned long prevmillis = 0;

MPU6050 mpu6500;

// Function declarations
void OnDataSent(const uint8_t *mac_addr, esp_now_send_status_t status);
void setupWifi();
int32_t getWiFiChannel(const char *ssid);
void innitMPU6500();
void recordData6500();
template<typename WireType = TwoWire>
uint8_t readByte(uint8_t address, uint8_t subAddress, WireType &wire = Wire);

typedef struct struct_message {
  int16_t ax1, ay1, az1, gx1, gy1, gz1;
  float qw1, qx1, qy1, qz1;
  float wax1, way1, waz1, wgx1, wgy1, wgz1;
  float temp1;
  int button1, button2, button3;
} struct_message;

struct_message wristData;
esp_now_peer_info_t peerInfo;

// Setup script
void setup() {
  Serial.begin(115200);

  // Setup Wires for both MPU I2C communication
  Wire.begin();
  Wire.setClock(400000);

  pinMode(BUTTON_PIN_1, INPUT_PULLUP);
  pinMode(BUTTON_PIN_2, INPUT_PULLUP);
  pinMode(BUTTON_PIN_3, INPUT_PULLUP);
  pinMode(SLEEP_PIN, INPUT_PULLUP);

  pinMode(WIFI_LED, OUTPUT);

  // Find if we use MPU9250 or MPU6500 to initialize the right modules
  byte ca = readByte(0x68, 0x75, Wire);
  if (ca == 0x70) {
    Serial.println("Buttons with MPU6500");
    innitMPU6500();
  } else {
    Serial.println("Buttons MPU ERROR");
    for (int i = 0; i < 40; i++) {
      digitalWrite(WIFI_LED, HIGH);
      delay(62);
      digitalWrite(WIFI_LED, LOW);
      delay(62);
    }
  }

  setupWifi();

  esp_sleep_enable_ext1_wakeup(0x302000000, ESP_EXT1_WAKEUP_ALL_LOW);
}

void loop() {
  // 10ms between datapoints
  unsigned long currentmillis = millis();
  if (currentmillis - prevmillis >= 10) {

    prevmillis = currentmillis;
    recordData6500();

    if (digitalRead(SLEEP_PIN) == LOW) {
      Serial.println("Going to sleep...");
      esp_deep_sleep_start();
    }
  }
}

void OnDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {
  digitalWrite(WIFI_LED, status == ESP_NOW_SEND_SUCCESS ? HIGH : LOW);
}

// Initialize MPU6500
void innitMPU6500() {

  mpu6500.initialize();
  mpu6500.dmpInitialize();

  mpu6500.setXAccelOffset(-4491);
  mpu6500.setYAccelOffset(-4534);
  mpu6500.setZAccelOffset(12577);
  mpu6500.setXGyroOffset(-32);
  mpu6500.setYGyroOffset(-524);
  mpu6500.setZGyroOffset(33);
  mpu6500.setFullScaleAccelRange(MPU6050_ACCEL_FS_8);
  mpu6500.setFullScaleGyroRange(MPU6050_GYRO_FS_1000);

  mpu6500.setDMPEnabled(true);
}


int32_t getWiFiChannel(const char *ssid) {
  if (int32_t n = WiFi.scanNetworks()) {
    for (uint8_t i = 0; i < n; i++) {
      if (!strcmp(ssid, WiFi.SSID(i).c_str())) {
        return WiFi.channel(i);
      }
    }
  }
  return 0;
}

void setupWifi() {
  WiFi.mode(WIFI_STA);

  int32_t channel = getWiFiChannel(WIFI_SSID);

  esp_wifi_set_promiscuous(true);
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);
  esp_wifi_set_promiscuous(false);

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
  esp_now_register_send_cb(OnDataSent);

  memcpy(peerInfo.peer_addr, broadcastAddress, 6);
  peerInfo.channel = 0;
  peerInfo.encrypt = false;

  // Add peer
  while (esp_now_add_peer(&peerInfo) != ESP_OK) {
    Serial.println("Failed to add peer");
  }
}

// Function to get the bits recognizing the MPU type
template<typename WireType>
uint8_t readByte(uint8_t address, uint8_t subAddress, WireType &wire) {
  uint8_t data = 0;
  wire.beginTransmission(address);
  wire.write(subAddress);
  wire.endTransmission(false);
  wire.requestFrom(address, (size_t)1);
  if (wire.available()) data = wire.read();
  return data;
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

  wristData.ax1 = aa.x;
  wristData.ay1 = aa.y;
  wristData.az1 = aa.z;
  wristData.gx1 = gyro.x;
  wristData.gy1 = gyro.y;
  wristData.gz1 = gyro.z;
  wristData.qw1 = q.w;
  wristData.qx1 = q.x;
  wristData.qy1 = q.y;
  wristData.qz1 = q.z;
  wristData.wax1 = aaWorld.x;
  wristData.way1 = aaWorld.y;
  wristData.waz1 = aaWorld.z;
  wristData.wgx1 = ggWorld.x;
  wristData.wgy1 = ggWorld.y;
  wristData.wgz1 = ggWorld.z;
  wristData.temp1 = mpu6500.getTemperature() / 340.00 + 36.53;
  wristData.button1 = digitalRead(BUTTON_PIN_1) == LOW ? 1 : 0;
  wristData.button2 = digitalRead(BUTTON_PIN_2) == LOW ? 1 : 0;
  wristData.button3 = digitalRead(BUTTON_PIN_3) == LOW ? 1 : 0;

  esp_err_t result = esp_now_send(broadcastAddress, (uint8_t *)&wristData, sizeof(wristData));
  if (result != ESP_OK) {
    Serial.print("*");
  }
}
