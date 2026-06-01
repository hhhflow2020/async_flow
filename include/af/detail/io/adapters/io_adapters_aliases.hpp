#pragma once

template <typename ThreadT> using TcpStream = IoStream<ThreadT>;

template <typename ThreadT> using TcpListener = IoListener<ThreadT>;

template <typename ThreadT> using UdpSocket = IoDatagramSocket<ThreadT>;
