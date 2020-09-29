import { OnChanges, SimpleChanges } from '@angular/core';
export interface NzSliderTrackStyle {
    bottom?: string | null;
    height?: string | null;
    left?: string | null;
    width?: string | null;
    visibility?: string;
}
export declare class NzSliderTrackComponent implements OnChanges {
    nzOffset: number;
    nzLength: number;
    nzVertical: boolean;
    nzIncluded: boolean;
    style: NzSliderTrackStyle;
    ngOnChanges(changes: SimpleChanges): void;
}
