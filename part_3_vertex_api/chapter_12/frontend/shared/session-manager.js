// session-manager.js
// Kullanıcıya özel session yönetimi için yardımcı fonksiyonlar

export class SessionManager {
  constructor(proxyBaseUrl = 'https://vertex-proxy-service-638345404110.us-central1.run.app') {
    this.proxyBaseUrl = proxyBaseUrl;
    this.currentSessionId = null;
    this.currentUserId = null;
  }

  // Session'ı localStorage'dan yükle veya yeni oluştur
  async initializeSession() {
    this.currentSessionId = localStorage.getItem('sessionId');
    this.currentUserId = localStorage.getItem('userId');
    if (!this.currentSessionId) {
      await this.createNewSession();
    } else {
      await this.checkSessionValidity();
    }
    return {
      sessionId: this.currentSessionId,
      userId: this.currentUserId
    };
  }

  async createNewSession(userId = null) {
    try {
      const response = await fetch(`${this.proxyBaseUrl}/create_session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: userId || this.currentUserId || `user_${Date.now()}` })
      });
      if (response.ok) {
        const sessionData = await response.json();
        this.currentSessionId = sessionData.session_id;
        this.currentUserId = sessionData.user_id;
        localStorage.setItem('sessionId', this.currentSessionId);
        localStorage.setItem('userId', this.currentUserId);
        return sessionData;
      } else {
        throw new Error('Failed to create session');
      }
    } catch (error) {
      throw error;
    }
  }

  async checkSessionValidity() {
    try {
      const response = await fetch(`${this.proxyBaseUrl}/session_info?session_id=${this.currentSessionId}`);
      if (!response.ok) {
        localStorage.removeItem('sessionId');
        await this.createNewSession();
      } else {
        return await response.json();
      }
    } catch (error) {
      await this.createNewSession();
    }
  }

  getWebSocketUrl(baseUrl) {
    if (this.currentSessionId) {
      return `${baseUrl}?session_id=${this.currentSessionId}&user_id=${this.currentUserId}`;
    }
    return baseUrl;
  }

  clearSession() {
    localStorage.removeItem('sessionId');
    localStorage.removeItem('userId');
    this.currentSessionId = null;
    this.currentUserId = null;
  }

  getCurrentSession() {
    return {
      sessionId: this.currentSessionId,
      userId: this.currentUserId
    };
  }
}

export const sessionManager = new SessionManager(); 