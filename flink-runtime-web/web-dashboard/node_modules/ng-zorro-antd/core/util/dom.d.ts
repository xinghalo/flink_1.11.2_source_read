import { Observable } from 'rxjs';
/**
 * Silent an event by stopping and preventing it.
 */
export declare function silentEvent(e: Event): void;
export declare function getElementOffset(elem: HTMLElement): {
    top: number;
    left: number;
};
export declare function findFirstNotEmptyNode(element: HTMLElement): Node | null;
export declare function findLastNotEmptyNode(element: HTMLElement): Node | null;
export declare function reverseChildNodes(parent: HTMLElement): void;
export interface MouseTouchObserverConfig {
    end: string;
    move: string;
    pluckKey: string[];
    start: string;
    end$?: Observable<Event>;
    moveResolved$?: Observable<number>;
    startPlucked$?: Observable<number>;
    filter?(e: Event): boolean;
}
