package io.vepo.kafka.metadata.producer;

import java.util.Objects;

public class Mensagem {
    private int id;
    private String mensagem;
    private long timestamp;

    public Mensagem() {
    }

    public Mensagem(int id, String mensagem, long timestamp) {
        this.id = id;
        this.mensagem = mensagem;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMensagem() {
        return mensagem;
    }

    public void setMensagem(String mensagem) {
        this.mensagem = mensagem;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, mensagem, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Mensagem other = (Mensagem) obj;
        return id == other.id && Objects.equals(mensagem, other.mensagem) && timestamp == other.timestamp;
    }

    @Override
    public String toString() {
        return String.format("Mensagem [id=%s, mensagem=%s, timestamp=%s]", id, mensagem, timestamp);
    }

}
