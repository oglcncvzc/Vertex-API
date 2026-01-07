# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" Vertex AI Gemini Multimodal Live WebSockets Proxy Server """
import asyncio
import json
import ssl
import traceback
import websockets
import certifi
import google.auth
import os
import aiohttp
from aiohttp import web
from google.auth.transport.requests import Request
from websockets.legacy.protocol import WebSocketCommonProtocol
from websockets.legacy.server import WebSocketServerProtocol
from google.oauth2 import service_account


print("DEBUG: proxy.py - Starting script...")  # Add print here


HOST = "us-central1-aiplatform.googleapis.com"
SERVICE_URL = f"wss://{HOST}/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"

DEBUG = True

# Track active connections
active_connections = set()



# Basit bir in-memory session store (örnek amaçlı, prod için uygun değil)
sessions = {}


async def get_access_token():
    """Retrieves the access token for the currently authenticated account."""
    try:
        # Cloud Run'da service account dosyası container içinde olacak
        SERVICE_ACCOUNT_FILE = "voice-asistant-459013-29c675d43902.json"
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(Request())
        print("Kullanılan service account:", creds.service_account_email)
        return creds.token
    except Exception as e:
        print(f"Error getting access token: {e}")
        print(f"Full traceback:\n{traceback.format_exc()}")
        raise


async def proxy_task(
    source_websocket: WebSocketCommonProtocol,
    target_websocket: WebSocketCommonProtocol,
    name: str = "",
) -> None:
    """
    Forwards messages from one WebSocket connection to another.
    """
    try:
        async for message in source_websocket:
            try:
                data = json.loads(message)

                # Log message type for debugging
                if "setup" in data:
                    print(f"{name} forwarding setup message")
                    print(f"Setup message content: {json.dumps(data, indent=2)}")
                elif "realtime_input" in data:
                    print(f"{name} forwarding audio/video input")
                elif "serverContent" in data:
                    has_audio = "inlineData" in str(data)
                    print(
                        f"{name} forwarding server content"
                        + (" with audio" if has_audio else "")
                    )
                else:
                    print(f"{name} forwarding message type: {list(data.keys())}")
                    print(f"Message content: {json.dumps(data, indent=2)}")

                # Forward the message
                try:
                    await target_websocket.send(json.dumps(data))
                except Exception as e:
                    print(f"\n{name} Error sending message:")
                    print("=" * 80)
                    print(f"Error details: {str(e)}")
                    print("=" * 80)
                    print(f"Message that failed: {json.dumps(data, indent=2)}")
                    raise

            except websockets.exceptions.ConnectionClosed as e:
                print(f"\n{name} connection closed during message processing:")
                print("=" * 80)
                print(f"Close code: {e.code}")
                print(f"Close reason (full):")
                print("-" * 40)
                print(e.reason)
                print("=" * 80)
                break
            except Exception as e:
                print(f"\n{name} Error processing message:")
                print("=" * 80)
                print(f"Error details: {str(e)}")
                print(f"Full traceback:\n{traceback.format_exc()}")
                print("=" * 80)

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n{name} connection closed:")
        print("=" * 80)
        print(f"Close code: {e.code}")
        print(f"Close reason (full):")
        print("-" * 40)
        print(e.reason)
        print("=" * 80)
    except Exception as e:
        print(f"\n{name} Error:")
        print("=" * 80)
        print(f"Error details: {str(e)}")
        print(f"Full traceback:\n{traceback.format_exc()}")
        print("=" * 80)
    finally:
        # Clean up connections when done
        print(f"{name} cleaning up connection")
        if target_websocket in active_connections:
            active_connections.remove(target_websocket)
        try:
            await target_websocket.close()
        except:
            pass


async def create_proxy(
    client_websocket, bearer_token: str
) -> None:
    """
    Establishes a WebSocket connection to the server and creates two tasks for
    bidirectional message forwarding between the client and the server.
    """
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        }

        print(f"Connecting to {SERVICE_URL}")
        async with websockets.connect(
            SERVICE_URL,
            extra_headers=headers,
            ssl=ssl.create_default_context(cafile=certifi.where()),
        ) as server_websocket:
            print("Connected to Vertex AI")
            active_connections.add(server_websocket)

            # Create bidirectional proxy tasks
            client_to_server = asyncio.create_task(
                proxy_task_aiohttp(client_websocket, server_websocket, "Client->Server")
            )
            server_to_client = asyncio.create_task(
                proxy_task_websockets(server_websocket, client_websocket, "Server->Client")
            )

            try:
                # Wait for both tasks to complete
                await asyncio.gather(client_to_server, server_to_client)
            except Exception as e:
                print(f"Error during proxy operation: {e}")
                print(f"Full traceback: {traceback.format_exc()}")
            finally:
                # Clean up tasks
                for task in [client_to_server, server_to_client]:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

    except Exception as e:
        print(f"Error creating proxy connection: {e}")
        print(f"Full traceback: {traceback.format_exc()}")


async def proxy_task_aiohttp(
    source_websocket,
    target_websocket: WebSocketCommonProtocol,
    name: str = "",
) -> None:
    """
    Forwards messages from aiohttp WebSocket to websockets WebSocket.
    """
    try:
        async for message in source_websocket:
            try:
                if message.type == web.WSMsgType.TEXT:
                    data = json.loads(message.data)
                elif message.type == web.WSMsgType.BINARY:
                    data = json.loads(message.data.decode())
                else:
                    continue

                # Log message type for debugging
                if "setup" in data:
                    print(f"{name} forwarding setup message")
                    print(f"Setup message content: {json.dumps(data, indent=2)}")
                elif "realtime_input" in data:
                    print(f"{name} forwarding audio/video input")
                elif "serverContent" in data:
                    has_audio = "inlineData" in str(data)
                    print(
                        f"{name} forwarding server content"
                        + (" with audio" if has_audio else "")
                    )
                else:
                    print(f"{name} forwarding message type: {list(data.keys())}")
                    print(f"Message content: {json.dumps(data, indent=2)}")

                # Forward the message
                try:
                    await target_websocket.send(json.dumps(data))
                except Exception as e:
                    print(f"\n{name} Error sending message:")
                    print("=" * 80)
                    print(f"Error details: {str(e)}")
                    print("=" * 80)
                    print(f"Message that failed: {json.dumps(data, indent=2)}")
                    raise

            except websockets.exceptions.ConnectionClosed as e:
                print(f"\n{name} connection closed during message processing:")
                print("=" * 80)
                print(f"Close code: {e.code}")
                print(f"Close reason (full):")
                print("-" * 40)
                print(e.reason)
                print("=" * 80)
                break
            except Exception as e:
                print(f"\n{name} Error processing message:")
                print("=" * 80)
                print(f"Error details: {str(e)}")
                print(f"Full traceback:\n{traceback.format_exc()}")
                print("=" * 80)

    except Exception as e:
        print(f"\n{name} Error:")
        print("=" * 80)
        print(f"Error details: {str(e)}")
        print(f"Full traceback:\n{traceback.format_exc()}")
        print("=" * 80)
    finally:
        # Clean up connections when done
        print(f"{name} cleaning up connection")
        if target_websocket in active_connections:
            active_connections.remove(target_websocket)
        try:
            await target_websocket.close()
        except:
            pass


async def proxy_task_websockets(
    source_websocket: WebSocketCommonProtocol,
    target_websocket,
    name: str = "",
) -> None:
    """
    Forwards messages from websockets WebSocket to aiohttp WebSocket.
    """
    try:
        async for message in source_websocket:
            try:
                data = json.loads(message)

                # Log message type for debugging
                if "setup" in data:
                    print(f"{name} forwarding setup message")
                    print(f"Setup message content: {json.dumps(data, indent=2)}")
                elif "realtime_input" in data:
                    print(f"{name} forwarding audio/video input")
                elif "serverContent" in data:
                    has_audio = "inlineData" in str(data)
                    print(
                        f"{name} forwarding server content"
                        + (" with audio" if has_audio else "")
                    )
                else:
                    print(f"{name} forwarding message type: {list(data.keys())}")
                    print(f"Message content: {json.dumps(data, indent=2)}")

                # Forward the message
                try:
                    await target_websocket.send_str(json.dumps(data))
                except Exception as e:
                    print(f"\n{name} Error sending message:")
                    print("=" * 80)
                    print(f"Error details: {str(e)}")
                    print("=" * 80)
                    print(f"Message that failed: {json.dumps(data, indent=2)}")
                    raise

            except websockets.exceptions.ConnectionClosed as e:
                print(f"\n{name} connection closed during message processing:")
                print("=" * 80)
                print(f"Close code: {e.code}")
                print(f"Close reason (full):")
                print("-" * 40)
                print(e.reason)
                print("=" * 80)
                break
            except Exception as e:
                print(f"\n{name} Error processing message:")
                print("=" * 80)
                print(f"Error details: {str(e)}")
                print(f"Full traceback:\n{traceback.format_exc()}")
                print("=" * 80)

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n{name} connection closed:")
        print("=" * 80)
        print(f"Close code: {e.code}")
        print(f"Close reason (full):")
        print("-" * 40)
        print(e.reason)
        print("=" * 80)
    except Exception as e:
        print(f"\n{name} Error:")
        print("=" * 80)
        print(f"Error details: {str(e)}")
        print(f"Full traceback:\n{traceback.format_exc()}")
        print("=" * 80)
    finally:
        # Clean up connections when done
        print(f"{name} cleaning up connection")
        if source_websocket in active_connections:
            active_connections.remove(source_websocket)
        try:
            await source_websocket.close()
        except:
            pass


async def handle_client(request):
    """
    Handles a new client connection.
    """
    print("New WebSocket connection...")
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    try:
        # Get auth token automatically
        bearer_token = await get_access_token()
        print("Retrieved bearer token automatically")

        # Send auth complete message to client
        await ws.send_json({"authComplete": True})
        print("Sent auth complete message")

        print("Creating proxy connection")
        await create_proxy(ws, bearer_token)

    except asyncio.TimeoutError:
        print("Timeout in handle_client")
        await ws.close(code=1008, message=b"Auth timeout")
    except Exception as e:
        print(f"Error in handle_client: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        await ws.close(code=1011, message=str(e).encode())
    
    return ws


async def cleanup_connections() -> None:
    """
    Periodically clean up stale connections
    """
    while True:
        print(f"Active connections: {len(active_connections)}")
        for conn in list(active_connections):
            try:
                await conn.ping()
            except:
                print("Found stale connection, removing...")
                active_connections.remove(conn)
                try:
                    await conn.close()
                except:
                    pass
        await asyncio.sleep(30)  # Check every 30 seconds


async def get_weather_data(city):
    """Hava durumu verilerini OpenWeatherMap API'sinden alır"""
    try:
        # Önce şehir için koordinatları al
        geo_url = f"https://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={OPENWEATHER_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(geo_url) as response:
                if response.status != 200:
                    return {"error": f"Geo API failed with status: {response.status}"}
                geo_data = await response.json()

        if not geo_data:
            return {"error": f"Could not find location: {city}"}

        lat, lon = geo_data[0]['lat'], geo_data[0]['lon']

        # Sonra hava durumu verilerini al
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={OPENWEATHER_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(weather_url) as response:
                if response.status != 200:
                    return {"error": f"Weather API failed with status: {response.status}"}
                weather_data = await response.json()

        return {
            "temperature": weather_data['main']['temp'],
            "description": weather_data['weather'][0]['description'],
            "humidity": weather_data['main']['humidity'],
            "windSpeed": weather_data['wind']['speed'],
            "city": weather_data['name'],
            "country": weather_data['sys']['country']
        }
    except Exception as e:
        return {"error": f"Error fetching weather for {city}: {str(e)}"}


async def weather_handler(request):
    """Hava durumu endpoint handler'ı"""
    try:
        # CORS headers
        response = web.Response()
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        
        if request.method == 'OPTIONS':
            response.status = 200
            return response

        # Şehir parametresini al
        city = request.query.get('city')
        if not city:
            response.status = 400
            response.text = json.dumps({"error": "City parameter is required"})
            response.content_type = 'application/json'
            return response

        # Hava durumu verilerini al
        weather_data = await get_weather_data(city)
        
        response.text = json.dumps(weather_data)
        response.content_type = 'application/json'
        return response
        
    except Exception as e:
        response = web.Response()
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.status = 500
        response.text = json.dumps({"error": f"Internal server error: {str(e)}"})
        response.content_type = 'application/json'
        return response


async def create_session_handler(request):
    # CORS headers
    response = web.Response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    
    if request.method == 'OPTIONS':
        response.set_status(200)
        return response
    try:
        data = await request.json()
        user_id = data.get('user_id')
        if not user_id:
            response.set_status(400)
            response.text = json.dumps({'error': 'user_id is required'})
            response.content_type = 'application/json'
            return response
        session_id = f"session_{user_id}_{len(sessions)+1}"
        sessions[session_id] = {'user_id': user_id}
        response.text = json.dumps({'session_id': session_id, 'user_id': user_id})
        response.content_type = 'application/json'
        return response
    except Exception as e:
        response.set_status(500)
        response.text = json.dumps({'error': str(e)})
        response.content_type = 'application/json'
        return response


async def session_info_handler(request):
    # CORS headers
    response = web.Response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    
    if request.method == 'OPTIONS':
        response.set_status(200)
        return response
    try:
        session_id = request.query.get('session_id')
        if not session_id or session_id not in sessions:
            response.set_status(404)
            response.text = json.dumps({'error': 'Session not found'})
            response.content_type = 'application/json'
            return response
        user_id = sessions[session_id]['user_id']
        response.text = json.dumps({'valid': True, 'user_id': user_id})
        response.content_type = 'application/json'
        return response
    except Exception as e:
        response.set_status(500)
        response.text = json.dumps({'error': str(e)})
        response.content_type = 'application/json'
        return response


async def main() -> None:
    """
    Starts the WebSocket server and HTTP server.
    """
    print(f"DEBUG: proxy.py - main() function started")
    # Cloud Run'da PORT environment variable'ını kullan
    port = int(os.environ.get("PORT", 8080))

    # Start the cleanup task
    cleanup_task = asyncio.create_task(cleanup_connections())

    # HTTP ve WebSocket sunucusu için app oluştur
    app = web.Application()
    app.router.add_get('/weather', weather_handler)
    app.router.add_post('/weather', weather_handler)
    app.router.add_options('/weather', weather_handler)
    
    # Yeni session endpointleri
    app.router.add_post('/create_session', create_session_handler)
    app.router.add_options('/create_session', create_session_handler)
    app.router.add_get('/session_info', session_info_handler)
    app.router.add_options('/session_info', session_info_handler)

    # WebSocket handler'ı ekle
    app.router.add_get('/ws', handle_client)

    # Sunucuyu başlat
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"HTTP and WebSocket server running on 0.0.0.0:{port}...")

    try:
        await asyncio.Future()  # run forever
    finally:
        cleanup_task.cancel()
        # Close all remaining connections
        for conn in list(active_connections):
            try:
                await conn.close()
            except:
                pass
        active_connections.clear()
        # Stop HTTP server
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
