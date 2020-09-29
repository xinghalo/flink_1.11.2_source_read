/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, state, style, transition, trigger } from '@angular/animations';
/** @type {?} */
export var notificationMotion = trigger('notificationMotion', [
    state('enterRight', style({ opacity: 1, transform: 'translateX(0)' })),
    transition('* => enterRight', [
        style({ opacity: 0, transform: 'translateX(5%)' }),
        animate('100ms linear')
    ]),
    state('enterLeft', style({ opacity: 1, transform: 'translateX(0)' })),
    transition('* => enterLeft', [
        style({ opacity: 0, transform: 'translateX(-5%)' }),
        animate('100ms linear')
    ]),
    state('leave', style({
        opacity: 0,
        transform: 'scaleY(0.8)',
        transformOrigin: '0% 0%'
    })),
    transition('* => leave', [
        style({
            opacity: 1,
            transform: 'scaleY(1)',
            transformOrigin: '0% 0%'
        }),
        animate('100ms linear')
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibm90aWZpY2F0aW9uLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvYW5pbWF0aW9uL25vdGlmaWNhdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLE9BQU8sRUFDUCxLQUFLLEVBQ0wsS0FBSyxFQUNMLFVBQVUsRUFDVixPQUFPLEVBRVIsTUFBTSxxQkFBcUIsQ0FBQzs7QUFFN0IsTUFBTSxLQUFPLGtCQUFrQixHQUE2QixPQUFPLENBQUMsb0JBQW9CLEVBQUU7SUFDeEYsS0FBSyxDQUFDLFlBQVksRUFBRSxLQUFLLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLFNBQVMsRUFBRSxlQUFlLEVBQUUsQ0FBQyxDQUFDO0lBQ3RFLFVBQVUsQ0FBQyxpQkFBaUIsRUFBRTtRQUM1QixLQUFLLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLFNBQVMsRUFBRSxnQkFBZ0IsRUFBRSxDQUFDO1FBQ2xELE9BQU8sQ0FBQyxjQUFjLENBQUM7S0FDeEIsQ0FBQztJQUNGLEtBQUssQ0FBQyxXQUFXLEVBQUUsS0FBSyxDQUFDLEVBQUUsT0FBTyxFQUFFLENBQUMsRUFBRSxTQUFTLEVBQUUsZUFBZSxFQUFFLENBQUMsQ0FBQztJQUNyRSxVQUFVLENBQUMsZ0JBQWdCLEVBQUU7UUFDM0IsS0FBSyxDQUFDLEVBQUUsT0FBTyxFQUFFLENBQUMsRUFBRSxTQUFTLEVBQUUsaUJBQWlCLEVBQUUsQ0FBQztRQUNuRCxPQUFPLENBQUMsY0FBYyxDQUFDO0tBQ3hCLENBQUM7SUFDRixLQUFLLENBQUMsT0FBTyxFQUFFLEtBQUssQ0FBQztRQUNuQixPQUFPLEVBQVUsQ0FBQztRQUNsQixTQUFTLEVBQVEsYUFBYTtRQUM5QixlQUFlLEVBQUUsT0FBTztLQUN6QixDQUFDLENBQUM7SUFDSCxVQUFVLENBQUMsWUFBWSxFQUFFO1FBQ3ZCLEtBQUssQ0FBQztZQUNKLE9BQU8sRUFBVSxDQUFDO1lBQ2xCLFNBQVMsRUFBUSxXQUFXO1lBQzVCLGVBQWUsRUFBRSxPQUFPO1NBQ3pCLENBQUM7UUFDRixPQUFPLENBQUMsY0FBYyxDQUFDO0tBQ3hCLENBQUM7Q0FDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgYW5pbWF0ZSxcbiAgc3RhdGUsXG4gIHN0eWxlLFxuICB0cmFuc2l0aW9uLFxuICB0cmlnZ2VyLFxuICBBbmltYXRpb25UcmlnZ2VyTWV0YWRhdGFcbn0gZnJvbSAnQGFuZ3VsYXIvYW5pbWF0aW9ucyc7XG5cbmV4cG9ydCBjb25zdCBub3RpZmljYXRpb25Nb3Rpb246IEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YSA9IHRyaWdnZXIoJ25vdGlmaWNhdGlvbk1vdGlvbicsIFtcbiAgc3RhdGUoJ2VudGVyUmlnaHQnLCBzdHlsZSh7IG9wYWNpdHk6IDEsIHRyYW5zZm9ybTogJ3RyYW5zbGF0ZVgoMCknIH0pKSxcbiAgdHJhbnNpdGlvbignKiA9PiBlbnRlclJpZ2h0JywgW1xuICAgIHN0eWxlKHsgb3BhY2l0eTogMCwgdHJhbnNmb3JtOiAndHJhbnNsYXRlWCg1JSknIH0pLFxuICAgIGFuaW1hdGUoJzEwMG1zIGxpbmVhcicpXG4gIF0pLFxuICBzdGF0ZSgnZW50ZXJMZWZ0Jywgc3R5bGUoeyBvcGFjaXR5OiAxLCB0cmFuc2Zvcm06ICd0cmFuc2xhdGVYKDApJyB9KSksXG4gIHRyYW5zaXRpb24oJyogPT4gZW50ZXJMZWZ0JywgW1xuICAgIHN0eWxlKHsgb3BhY2l0eTogMCwgdHJhbnNmb3JtOiAndHJhbnNsYXRlWCgtNSUpJyB9KSxcbiAgICBhbmltYXRlKCcxMDBtcyBsaW5lYXInKVxuICBdKSxcbiAgc3RhdGUoJ2xlYXZlJywgc3R5bGUoe1xuICAgIG9wYWNpdHkgICAgICAgIDogMCxcbiAgICB0cmFuc2Zvcm0gICAgICA6ICdzY2FsZVkoMC44KScsXG4gICAgdHJhbnNmb3JtT3JpZ2luOiAnMCUgMCUnXG4gIH0pKSxcbiAgdHJhbnNpdGlvbignKiA9PiBsZWF2ZScsIFtcbiAgICBzdHlsZSh7XG4gICAgICBvcGFjaXR5ICAgICAgICA6IDEsXG4gICAgICB0cmFuc2Zvcm0gICAgICA6ICdzY2FsZVkoMSknLFxuICAgICAgdHJhbnNmb3JtT3JpZ2luOiAnMCUgMCUnXG4gICAgfSksXG4gICAgYW5pbWF0ZSgnMTAwbXMgbGluZWFyJylcbiAgXSlcbl0pO1xuIl19