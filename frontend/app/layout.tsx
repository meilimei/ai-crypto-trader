import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AI Crypto Trader",
  description: "AI-assisted crypto trading dashboard"
};

type RootLayoutProps = {
  children: React.ReactNode;
};

export default function RootLayout({ children }: RootLayoutProps): JSX.Element {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
