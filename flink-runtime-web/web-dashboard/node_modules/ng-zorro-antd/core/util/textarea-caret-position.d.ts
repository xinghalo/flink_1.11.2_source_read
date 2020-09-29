export declare const properties: string[];
export interface Coordinates {
    top: number;
    left: number;
    height: number;
}
export declare function getCaretCoordinates(element: HTMLInputElement | HTMLTextAreaElement, position: number, options?: {
    debug?: boolean;
}): Coordinates;
export declare function createDebugEle(element: HTMLInputElement | HTMLTextAreaElement, coordinates: Coordinates): void;
